package com.github.kevindrosendahl.javaannbench.bench;

import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.index.Index;
import com.google.common.base.Preconditions;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import oshi.SystemInfo;

public class Recall {

  private static final int WARMUP_ITERATIONS = 2;
  private static final int TEST_ITERATIONS = 3;

  public record Results(
      DescriptiveStatistics recall,
      DescriptiveStatistics executionDurationMicros,
      DescriptiveStatistics minorFaults,
      DescriptiveStatistics majorFaults) {}

  public static Results test(
      Index.Querier index,
      RandomAccessVectorValues<float[]> queries,
      int k,
      List<List<Integer>> groundTruths)
      throws IOException {
    var systemInfo = new SystemInfo();
    var process = systemInfo.getOperatingSystem().getCurrentProcess();
    var numQueries = queries.size();

    try (var progress = ProgressBar.create("warmup", WARMUP_ITERATIONS * numQueries)) {
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        for (int j = 0; j < numQueries; j++) {
          var query = queries.vectorValue(j);
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
          var query = queries.vectorValue(j);

          Preconditions.checkArgument(process.updateAttributes(), "failed to update process stats");
          var startMinorFaults = process.getMinorFaults();
          var startMajorFaults = process.getMajorFaults();

          var start = Instant.now();
          var results = index.query(query, k);
          var end = Instant.now();

          Preconditions.checkArgument(process.updateAttributes(), "failed to update process stats");
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

          var groundTruth = groundTruths.get(j);
          var truePositives = groundTruth.stream().limit(k).filter(results::contains).count();
          var recall = (double) truePositives / k;
          recalls.addValue(recall);

          minorFaults.addValue(endMinorFaults - startMinorFaults);
          majorFaults.addValue(endMajorFaults - startMajorFaults);

          progress.inc();
        }
      }
    }

    return new Results(recalls, executionDurations, minorFaults, majorFaults);
  }
}
