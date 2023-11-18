package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.dataset.Datasets;
import com.github.kevindrosendahl.javaannbench.display.Progress;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.github.kevindrosendahl.javaannbench.util.Exceptions;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
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

  private static final int WARMUP_ITERATIONS = 2;
  private static final int TEST_ITERATIONS = 3;

  public static void test(QuerySpec spec, Path datasetsPath, Path indexesPath, Path reportsPath)
      throws Exception {
    var dataset = Datasets.load(datasetsPath, spec.dataset());
    try (var index =
        Index.Querier.fromParameters(
            dataset, indexesPath, spec.provider(), spec.type(), spec.build(), spec.query())) {

      var systemInfo = new SystemInfo();
      var numQueries = dataset.test().size();
      var queries = new ArrayList<float[]>(numQueries);
      int k = spec.k();

      for (int i = 0; i < numQueries; i++) {
        queries.add(dataset.test().vectorValue(i));
      }

      try (var pool = new ForkJoinPool(spec.runtime().queryThreads())) {
        try (var progress = ProgressBar.create("warmup", WARMUP_ITERATIONS * numQueries)) {
          pool.submit(
                  () -> {
                    IntStream.range(0, WARMUP_ITERATIONS)
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
        }

        var recalls = new SynchronizedDescriptiveStatistics();
        var executionDurations = new SynchronizedDescriptiveStatistics();
        var minorFaults = new SynchronizedDescriptiveStatistics();
        var majorFaults = new SynchronizedDescriptiveStatistics();

        try (var progress = ProgressBar.create("testing", TEST_ITERATIONS * numQueries)) {
          pool.submit(
                  () -> {
                    IntStream.range(0, TEST_ITERATIONS)
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
                                                  spec.runtime().queryThreads() != 1,
                                                  progress);
                                            });
                                      });
                            });
                  })
              .join();
        }

        LOGGER.info("completed recall test for {}:", index.description());
        LOGGER.info("\ttotal queries {}", recalls.getN());
        LOGGER.info("\taverage recall {}", recalls.getMean());
        LOGGER.info("\taverage duration {}", Duration.ofNanos((long) executionDurations.getMean()));
        LOGGER.info("\taverage minor faults {}", minorFaults.getMean());
        LOGGER.info("\taverage major faults {}", majorFaults.getMean());
        LOGGER.info("\tmax duration {}", Duration.ofNanos((long) executionDurations.getMax()));
        LOGGER.info("\tmax minor faults {}", minorFaults.getMax());
        LOGGER.info("\tmax major faults {}", majorFaults.getMax());
        LOGGER.info("\ttotal minor faults {}", minorFaults.getSum());
        LOGGER.info("\ttotal major faults {}", majorFaults.getSum());

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
      Progress progress)
      throws Exception {
    boolean collectThreadStats = systemInfo.getOperatingSystem().getFamily() != "macOS";

    StatsCollector statsCollector =
        (collectThreadStats && concurrent)
            ? new ThreadStatsCollector(systemInfo)
            : new ProcessStatsCollector(systemInfo);
    var startMinorFaults = 0L;
    var startMajorFaults = 0L;
    if (collectThreadStats) {
      Preconditions.checkArgument(statsCollector.update(), "failed to update stats");
      startMinorFaults = statsCollector.minorFaults();
      startMajorFaults = statsCollector.majorFaults();
    }

    var start = Instant.now();
    var results = index.query(query, k);
    var end = Instant.now();

    var endMinorFaults = 0L;
    var endMajorFaults = 0L;
    if (collectThreadStats) {
      Preconditions.checkArgument(statsCollector.update(), "failed to update thread stats");
      endMinorFaults = statsCollector.minorFaults();
      endMajorFaults = statsCollector.majorFaults();
    }

    var duration = Duration.between(start, end);
    executionDurations.addValue(duration.toNanos());

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

    minorFaults.addValue(endMinorFaults - startMinorFaults);
    majorFaults.addValue(endMajorFaults - startMajorFaults);

    progress.inc();
  }

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
            spec.runtime().systemMemory(),
            spec.runtime().heapSize(),
            Integer.toString(spec.runtime().queryThreads()),
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
}
