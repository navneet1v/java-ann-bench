package com.github.kevindrosendahl.javaannbench.bench;

import com.github.kevindrosendahl.javaannbench.index.Index;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import oshi.SystemInfo;

public class Recall {

  public record Results(DescriptiveStatistics recall, DescriptiveStatistics executionDurationMicros,
                        DescriptiveStatistics minorFaults, DescriptiveStatistics majorFaults) {

  }

  public static Results test(Index.Querier index, List<float[]> queries, int k,
      List<List<Integer>> groundTruths) throws IOException {
    var systemInfo = new SystemInfo();
    var process = systemInfo.getOperatingSystem().getCurrentProcess();

    var numQueries = queries.size();
    var recalls = new DescriptiveStatistics();
    var executionDurations = new DescriptiveStatistics();
    var minorFaults = new DescriptiveStatistics();
    var majorFaults = new DescriptiveStatistics();

    for (int i = 0; i < numQueries; i++) {
      var query = queries.get(i);

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

      var groundTruth = groundTruths.get(i);
      var truePositives = groundTruth.stream().limit(k).filter(results::contains).count();
      var recall = (double) truePositives / k;
      recalls.addValue(recall);

      minorFaults.addValue(endMinorFaults - startMinorFaults);
      majorFaults.addValue(endMajorFaults - startMajorFaults);
    }

    return new Results(recalls, executionDurations, minorFaults, majorFaults);
  }

}
