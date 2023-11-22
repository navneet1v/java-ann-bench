package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.dataset.Datasets;
import com.github.kevindrosendahl.javaannbench.display.Progress;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.github.kevindrosendahl.javaannbench.util.Exceptions;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

      var queryThreads = queryThreads(spec.runtime());
      var concurrent = queryThreads != 1;
      var systemInfo = new SystemInfo();
      var numQueries = dataset.test().size();
      var queries = new ArrayList<float[]>(numQueries);
      var warmup = warmup(spec.runtime());
      var test = test(spec.runtime());
      var k = spec.k();
      var jfr = jfr(spec.runtime());
      var recall = recall(spec.runtime());
      var threadStats = recall(spec.runtime());
      var blockDevice = blockDevice(spec.runtime());
      int blockDeviceStatsIntervalMs = blockDeviceStatsIntervalMs(spec.runtime());

      for (int i = 0; i < numQueries; i++) {
        queries.add(dataset.test().vectorValue(i));
      }

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
                                                index.query(query, k);
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
                index.query(query, k);
                progress.inc();
              }
            }
          }
        }

        var recalls = new SynchronizedDescriptiveStatistics();
        var executionDurations = new SynchronizedDescriptiveStatistics();
        var minorFaults = new SynchronizedDescriptiveStatistics();
        var majorFaults = new SynchronizedDescriptiveStatistics();

        Recording recording = null;
        if (jfr) {
          Configuration config = Configuration.getConfiguration("profile");
          recording = new Recording(config);
          recording.start();
        }
        DiskStatsCollector diskStatsCollector = null;
        if (blockDevice.isPresent()) {
          diskStatsCollector =
              collectDiskStats(systemInfo, blockDevice.get(), blockDeviceStatsIntervalMs);
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
                                                    progress);
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
                    progress);
              }
            }
          }
        }
        if (jfr) {
          recording.stop();
        }
        if (diskStatsCollector != null) {
          diskStatsCollector.latch.countDown();
          diskStatsCollector.latch.await();
          diskStatsCollector.future.get();
        }
        if (jfr) {
          var formatter =
              DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")
                  .withZone(ZoneId.of("America/Los_Angeles"));
          var jfrFileName = formatter.format(Instant.now()) + ".jfr";
          var jfrPath = reportsPath.resolve(jfrFileName);

          recording.dump(jfrPath);
          recording.close();

          LOGGER.info("wrote jfr recording {}", jfrFileName);
        }

        LOGGER.info("completed recall test for {}:", index.description());
        LOGGER.info("\ttotal queries {}", recalls.getN());
        if (recall) {
          LOGGER.info("\taverage recall {}", recalls.getMean());
        }
        LOGGER.info("\taverage duration {}", Duration.ofNanos((long) executionDurations.getMean()));
        if (threadStats) {
          LOGGER.info("\taverage minor faults {}", minorFaults.getMean());
          LOGGER.info("\taverage major faults {}", majorFaults.getMean());
        }
        if (diskStatsCollector != null) {
          LOGGER.info("\taverage queue length {}", diskStatsCollector.queueLength.getMean());
          LOGGER.info("\taverage transfer time {}", diskStatsCollector.transferTime.getMean());

          long samples = diskStatsCollector.reads.getN();
          int seconds = 1000 / ((int) samples * blockDeviceStatsIntervalMs);

          double reads = diskStatsCollector.reads.getSum();
          double readRate = reads / seconds;
          LOGGER.info("\taverage reads {}/s", readRate);

          double readBytes = diskStatsCollector.readBytes.getSum();
          double readBytesRate = readBytes / seconds;
          Bytes readBytesRateBytes = Bytes.ofBytes((long) readBytesRate);
          LOGGER.info("\taverage read rate {}/s", readBytesRateBytes);
        }
        LOGGER.info("\tmax duration {}", Duration.ofNanos((long) executionDurations.getMax()));
        if (threadStats) {
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
      Progress progress)
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
    var results = index.query(query, k);
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

    progress.inc();
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

  private static boolean recall(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("recall")).map(Boolean::parseBoolean).orElse(true);
  }

  private static boolean threadStats(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("threadStats")).map(Boolean::parseBoolean).orElse(true);
  }

  private static boolean jfr(Map<String, String> runtime) {
    return Optional.ofNullable(runtime.get("jfr")).map(Boolean::parseBoolean).orElse(false);
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
