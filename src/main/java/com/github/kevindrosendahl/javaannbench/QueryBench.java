package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.dataset.Datasets;
import com.github.kevindrosendahl.javaannbench.display.Progress;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.github.kevindrosendahl.javaannbench.util.Exceptions;
import com.google.common.base.Preconditions;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OSThread;

public class QueryBench {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryBench.class);

  private static final int DEFAULT_WARMUP_ITERATIONS = 1;
  private static final int DEFAULT_TEST_ITERATIONS = 2;
  private static final int DEFAULT_BLOCK_DEVICE_STATS_INTERVAL_MS = 10;

  public static void test(QuerySpec spec, Path datasetsPath, Path indexesPath, Path reportsPath)
      throws Exception {
    var dataset = Datasets.load(datasetsPath, spec.dataset());
    try (var index =
        Index.Querier.fromParameters(
            dataset, indexesPath, spec.provider(), spec.type(), spec.build(), spec.query())) {

      Thread.sleep(Duration.ofHours(10));
      var queryThreads = queryThreads(spec.runtime());
      var concurrent = queryThreads != 1;
      var systemInfo = new SystemInfo();
      var warmup = warmup(spec.runtime());
      var test = test(spec.runtime());
      var testOnTrain = testOnTrain(spec.runtime());
      var trainTestQueries = trainTestQueries(spec.runtime());
      var k = spec.k();
      var jfr = jfr(spec.runtime());
      var recall = recall(spec.runtime());
      var threadStats = threadStats(spec.runtime());
      var random = random(spec.runtime());
      var numQueries = testOnTrain ? trainTestQueries : dataset.test().size();
      var queries = new ArrayList<float[]>(numQueries);

      Preconditions.checkArgument(!(testOnTrain && recall));

      for (int i = 0; i < numQueries; i++) {
        float[] vector =
            testOnTrain
                ? dataset.train().vectorValue(random.nextInt(dataset.train().size()))
                : dataset.test().vectorValue(i);
        queries.add(vector);
      }

      var recalls = new SynchronizedDescriptiveStatistics();
      var executionDurations = new SynchronizedDescriptiveStatistics();
      var minorFaults = new SynchronizedDescriptiveStatistics();
      var majorFaults = new SynchronizedDescriptiveStatistics();

      try (var prom = startPromServer(spec, numQueries * test)) {
        try (var pool = new ForkJoinPool(queryThreads)) {
          try (var progress = ProgressBar.create("warmup", warmup * numQueries)) {
            if (concurrent) {
              pool.submit(
                      () -> {
                        IntStream.range(0, warmup)
                            .parallel()
                            .forEach(
                                i -> {
                                  IntStream.range(0, numQueries)
                                      .parallel()
                                      .forEach(
                                          j -> {
                                            Exceptions.wrap(
                                                () -> {
                                                  var query = queries.get(j);
                                                  index.query(query, k, recall);
                                                  progress.inc();
                                                });
                                          });
                                });
                      })
                  .join();
            } else {
              for (int i = 0; i < warmup; i++) {
                for (int j = 0; j < numQueries; j++) {
                  var query = queries.get(j);
                  index.query(query, k, recall);
                  progress.inc();
                }
              }
            }
          }

          Recording recording = null;
          if (jfr) {
            var formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")
                    .withZone(ZoneId.of("America/Los_Angeles"));
            var jfrFileName = formatter.format(Instant.now()) + ".jfr";
            var jfrPath = reportsPath.resolve(jfrFileName);
            LOGGER.info("starting jfr, will dump to {}", jfrFileName);
            Configuration config = Configuration.getConfiguration("profile");
            recording = new Recording(config);
            recording.setDestination(jfrPath);
            recording.setDumpOnExit(true);
            recording.start();
          }

          try (var progress = ProgressBar.create("testing", test * numQueries)) {
            if (concurrent) {
              pool.submit(
                      () -> {
                        IntStream.range(0, test)
                            .parallel()
                            .forEach(
                                i -> {
                                  IntStream.range(0, numQueries)
                                      .parallel()
                                      .forEach(
                                          j -> {
                                            Exceptions.wrap(
                                                () -> {
                                                  var query = queries.get(j);
                                                  List<Integer> groundTruth = null;
                                                  if (recall) {
                                                    groundTruth = dataset.groundTruth().get(j);
                                                  }
                                                  runQuery(
                                                      index,
                                                      query,
                                                      groundTruth,
                                                      spec.k(),
                                                      i,
                                                      j,
                                                      systemInfo,
                                                      recalls,
                                                      executionDurations,
                                                      minorFaults,
                                                      majorFaults,
                                                      concurrent,
                                                      recall,
                                                      threadStats,
                                                      progress,
                                                      prom.queryDurationSeconds);
                                                  prom.queries.inc();
                                                });
                                          });
                                });
                      })
                  .join();
            } else {
              for (int i = 0; i < test; i++) {
                for (int j = 0; j < numQueries; j++) {
                  var query = queries.get(j);
                  var groundTruth = dataset.groundTruth().get(j);
                  runQuery(
                      index,
                      query,
                      groundTruth,
                      spec.k(),
                      i,
                      j,
                      systemInfo,
                      recalls,
                      executionDurations,
                      minorFaults,
                      majorFaults,
                      concurrent,
                      recall,
                      threadStats,
                      progress,
                      prom.queryDurationSeconds);
                  prom.queries.inc();
                }
              }
              //              }
            }
          }
          if (jfr) {
            recording.stop();
            recording.close();
          }
        }

        LOGGER.info("completed recall test for {}:", index.description());
        LOGGER.info("\ttotal queries {}", recalls.getN());
        if (recall && !testOnTrain) {
          LOGGER.info("\taverage recall {}", recalls.getMean());
        }
        LOGGER.info("\taverage duration {}", Duration.ofNanos((long) executionDurations.getMean()));
        if (threadStats && !testOnTrain) {
          LOGGER.info("\taverage minor faults {}", minorFaults.getMean());
          LOGGER.info("\taverage major faults {}", majorFaults.getMean());
        }
        LOGGER.info("\tmax duration {}", Duration.ofNanos((long) executionDurations.getMax()));
        if (threadStats && !testOnTrain) {
          LOGGER.info("\tmax minor faults {}", minorFaults.getMax());
          LOGGER.info("\tmax major faults {}", majorFaults.getMax());
          LOGGER.info("\ttotal minor faults {}", minorFaults.getSum());
          LOGGER.info("\ttotal major faults {}", majorFaults.getSum());
        }

        new Report(index.description(), spec, recalls, executionDurations, minorFaults, majorFaults)
            .write(reportsPath);
      }
    }
  }

  private static void runQuery(
      Index.Querier index,
      float[] query,
      List<Integer> groundTruth,
      int k,
      int i,
      int j,
      SystemInfo systemInfo,
      DescriptiveStatistics recalls,
      DescriptiveStatistics executionDurations,
      DescriptiveStatistics minorFaults,
      DescriptiveStatistics majorFaults,
      boolean concurrent,
      boolean collectRecall,
      boolean threadStats,
      Progress progress,
      Gauge.Child queryDurationSeconds)
      throws Exception {
    boolean collectThreadStats = systemInfo.getOperatingSystem().getFamily() != "macOS";

    StatsCollector statsCollector =
        threadStats
            ? (collectThreadStats && concurrent)
                ? new ThreadStatsCollector(systemInfo)
                : new ProcessStatsCollector(systemInfo)
            : null;
    var startMinorFaults = 0L;
    var startMajorFaults = 0L;
    if (threadStats && collectThreadStats) {
      Preconditions.checkArgument(statsCollector.update(), "failed to update stats");
      startMinorFaults = statsCollector.minorFaults();
      startMajorFaults = statsCollector.majorFaults();
    }

    var start = Instant.now();
    var results = index.query(query, k, collectRecall);
    var end = Instant.now();

    var endMinorFaults = 0L;
    var endMajorFaults = 0L;
    if (threadStats && collectThreadStats) {
      Preconditions.checkArgument(statsCollector.update(), "failed to update thread stats");
      endMinorFaults = statsCollector.minorFaults();
      endMajorFaults = statsCollector.majorFaults();
    }

    var duration = Duration.between(start, end);
    executionDurations.addValue(duration.toNanos());

    if (collectRecall) {
      Preconditions.checkArgument(
          results.size() <= k,
          "query %s in round %s returned %s results, expected less than k=%s",
          j,
          i,
          results.size(),
          k);

      var truePositives = groundTruth.stream().limit(k).filter(results::contains).count();
      var recall = (double) truePositives / k;
      recalls.addValue(recall);
    }

    if (threadStats) {
      minorFaults.addValue(endMinorFaults - startMinorFaults);
      majorFaults.addValue(endMajorFaults - startMajorFaults);
    }

    queryDurationSeconds.inc((double) duration.toNanos() / (1000 * 1000 * 1000));
    progress.inc();
  }

  private static Prom startPromServer(QuerySpec spec, int numQueries) throws Exception {
    DefaultExports.initialize();

    Map<String, String> labels = new HashMap<>();
    labels.put("run_id", UUID.randomUUID().toString());
    labels.put("provider", spec.provider());
    labels.put("type", spec.type());
    labels.put("dataset", spec.dataset());
    labels.put("k", Integer.toString(spec.k()));
    spec.build().forEach((key, value) -> labels.put("build_" + key, value));
    spec.query().forEach((key, value) -> labels.put("query_" + key, value));
    spec.runtime().forEach((key, value) -> labels.put("runtime_" + key, value));
    String[] labelNames = labels.keySet().stream().sorted().toArray(String[]::new);
    String[] labelValues =
        labels.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toArray(String[]::new);

    Gauge.Child queries =
        Gauge.build()
            .labelNames(labelNames)
            .name("queries_total")
            .help("queries")
            .register()
            .labels(labelValues);

    Gauge.Child queryDurationSeconds =
        Gauge.build()
            .labelNames(labelNames)
            .name("query_duration_seconds")
            .help("queries")
            .register()
            .labels(labelValues);

    Gauge.build()
        .labelNames(labelNames)
        .name("num_queries")
        .help("num_queries")
        .register()
        .labels(labelValues)
        .set(numQueries);

    HTTPServer server = new HTTPServer(20000);
    return new Prom(server, queries, queryDurationSeconds, labelValues);
  }

  record Prom(
      HTTPServer server, Gauge.Child queries, Gauge.Child queryDurationSeconds, String[] labels)
      implements Closeable {

    @Override
    public void close() throws IOException {
      server.close();
    }
  }

  private static DiskStatsCollector collectDiskStats(
      SystemInfo info, String deviceName, int intervalMillis) {
    var latch = new CountDownLatch(1);
    var queueLength = new SynchronizedDescriptiveStatistics();
    var readBytes = new SynchronizedDescriptiveStatistics();
    var reads = new SynchronizedDescriptiveStatistics();
    var transferTime = new SynchronizedDescriptiveStatistics();

    var disk =
        info.getHardware().getDiskStores().stream()
            .filter(store -> store.getName().equals(deviceName))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("could not find device " + deviceName));

    var future =
        CompletableFuture.runAsync(
            () -> {
              Exceptions.wrap(
                  () -> {
                    disk.updateAttributes();
                    long totalBytesRead = disk.getReadBytes();
                    long totalReads = disk.getReads();

                    for (int i = 0; ; i++) {
                      boolean done = latch.await(intervalMillis, TimeUnit.MILLISECONDS);
                      if (done) {
                        return;
                      }

                      disk.updateAttributes();
                      queueLength.addValue((double) disk.getCurrentQueueLength());

                      long previousTotalBytesRead = totalBytesRead;
                      totalBytesRead = disk.getReadBytes();
                      readBytes.addValue((double) totalBytesRead - previousTotalBytesRead);

                      long previousTotalReads = totalReads;
                      totalReads = disk.getReads();
                      reads.addValue((double) totalReads - previousTotalReads);

                      transferTime.addValue((double) disk.getTransferTime());
                    }
                  });
            },
            Executors.newSingleThreadExecutor());

    return new DiskStatsCollector(latch, future, queueLength, readBytes, reads, transferTime);
  }

  private record DiskStatsCollector(
      CountDownLatch latch,
      CompletableFuture<?> future,
      DescriptiveStatistics queueLength,
      DescriptiveStatistics readBytes,
      DescriptiveStatistics reads,
      DescriptiveStatistics transferTime) {}

  private interface StatsCollector {
    boolean update();

    long minorFaults();

    long majorFaults();
  }

  private static class ThreadStatsCollector implements StatsCollector {

    private final OSThread thread;

    public ThreadStatsCollector(SystemInfo info) {
      this.thread = info.getOperatingSystem().getCurrentThread();
    }

    @Override
    public boolean update() {
      return thread.updateAttributes();
    }

    @Override
    public long minorFaults() {
      return thread.getMinorFaults();
    }

    @Override
    public long majorFaults() {
      return thread.getMajorFaults();
    }
  }

  private static class ProcessStatsCollector implements StatsCollector {

    private final OSProcess process;

    public ProcessStatsCollector(SystemInfo info) {
      this.process = info.getOperatingSystem().getCurrentProcess();
    }

    @Override
    public boolean update() {
      return process.updateAttributes();
    }

    @Override
    public long minorFaults() {
      return process.getMinorFaults();
    }

    @Override
    public long majorFaults() {
      return process.getMajorFaults();
    }
  }

  // FIXME: record full fidelity results, as well as some quantiles
  private record Report(
      String indexDescription,
      QuerySpec spec,
      DescriptiveStatistics recall,
      DescriptiveStatistics executionDurations,
      DescriptiveStatistics minorFaults,
      DescriptiveStatistics majorFaults) {

    void write(Path reportsPath) throws Exception {
      var now = Instant.now().getEpochSecond();
      var path =
          reportsPath.resolve(
              String.format("%s-query-%s-%s", now, spec.dataset(), indexDescription));
      var data =
          new String[] {
            "v1",
            indexDescription,
            spec.dataset(),
            spec.provider(),
            spec.type(),
            spec.buildString(),
            spec.queryString(),
            spec.runtimeString(),
            Double.toString(recall.getMean()),
            Long.toString((long) executionDurations.getMean()),
            Long.toString((long) executionDurations.getMax()),
            Double.toString(minorFaults.getMean()),
            Double.toString(minorFaults.getMax()),
            Double.toString(minorFaults.getSum()),
            Double.toString(majorFaults.getMean()),
            Double.toString(majorFaults.getMax()),
            Double.toString(majorFaults.getSum()),
          };

      try (var writer = Files.newBufferedWriter(path);
          var printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
        printer.printRecord((Object[]) data);
        printer.flush();
      }

      LOGGER.info("wrote report to {}", path);
    }
  }

  private static int queryThreads(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("queryThreads")).map(Integer::parseInt).orElse(1);
  }

  private static int warmup(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("warmup"))
        .map(Integer::parseInt)
        .orElse(DEFAULT_WARMUP_ITERATIONS);
  }

  private static int test(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("test"))
        .map(Integer::parseInt)
        .orElse(DEFAULT_TEST_ITERATIONS);
  }

  private static boolean testOnTrain(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("testOnTrain")).map(Boolean::parseBoolean).orElse(false);
  }

  private static int trainTestQueries(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("trainTestQueries"))
        .map(Integer::parseInt)
        .orElse(100000);
  }

  private static boolean recall(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("recall")).map(Boolean::parseBoolean).orElse(true);
  }

  private static boolean threadStats(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("threadStats")).map(Boolean::parseBoolean).orElse(true);
  }

  private static boolean jfr(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("jfr")).map(Boolean::parseBoolean).orElse(false);
  }

  private static Random random(Map<String, String> runtime) {
    int seed = Optional.ofNullable(runtime.get("seed")).map(Integer::parseInt).orElse(0);
    return new Random(seed);
  }

  private static Optional<String> blockDevice(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("blockDevice"));
  }

  private static int blockDeviceStatsIntervalMs(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("blockDeviceStatsIntervalMs"))
        .map(Integer::parseInt)
        .orElse(DEFAULT_BLOCK_DEVICE_STATS_INTERVAL_MS);
  }
}
