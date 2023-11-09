package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.dataset.Datasets;
import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;

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
      var process = systemInfo.getOperatingSystem().getCurrentProcess();
      var numQueries = dataset.train().size();
      var queries = new ArrayList<float[]>(numQueries);
      int k = spec.k();

      for (int i = 0; i < numQueries; i++) {
        queries.add(dataset.train().vectorValue(i));
      }

      try (var progress = ProgressBar.create("warmup", WARMUP_ITERATIONS * numQueries)) {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
          for (int j = 0; j < numQueries; j++) {
            var query = queries.get(j);
            index.query(query, k);
            progress.inc();
          }
        }
      }

      var recalls = new DescriptiveStatistics();
      var executionDurations = new DescriptiveStatistics();
      var minorFaults = new DescriptiveStatistics();
      var majorFaults = new DescriptiveStatistics();

      try (var progress = ProgressBar.create("testing", TEST_ITERATIONS * numQueries)) {
        for (int i = 0; i < TEST_ITERATIONS; i++) {
          for (int j = 0; j < numQueries; j++) {
            var query = queries.get(j);

            Preconditions.checkArgument(
                process.updateAttributes(), "failed to update process stats");
            var startMinorFaults = process.getMinorFaults();
            var startMajorFaults = process.getMajorFaults();

            var start = Instant.now();
            var results = index.query(query, spec.k());
            var end = Instant.now();

            Preconditions.checkArgument(
                process.updateAttributes(), "failed to update process stats");
            var endMinorFaults = process.getMinorFaults();
            var endMajorFaults = process.getMajorFaults();

            var duration = Duration.between(start, end);
            executionDurations.addValue(duration.toNanos());

            Preconditions.checkArgument(
                results.size() <= k,
                "query %s returned %s results, expected less than k=%s",
                j,
                results.size(),
                k);

            var groundTruth = dataset.groundTruth().get(j);
            var truePositives = groundTruth.stream().limit(k).filter(results::contains).count();
            var recall = (double) truePositives / k;
            recalls.addValue(recall);

            minorFaults.addValue(endMinorFaults - startMinorFaults);
            majorFaults.addValue(endMajorFaults - startMajorFaults);

            progress.inc();
          }
        }
      }

      LOGGER.info("completed recall test for {}:", index.description());
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

  private record Report(
      String indexDescription,
      QuerySpec spec,
      DescriptiveStatistics recall,
      DescriptiveStatistics executionDurations,
      DescriptiveStatistics minorFaults,
      DescriptiveStatistics majorFaults) {

    void write(Path reportsPath) throws Exception {
      var now = Instant.now().getEpochSecond();
      var path = reportsPath.resolve(String.format("%s-%s", now, indexDescription));
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
    }
  }
}
