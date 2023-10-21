package com.github.kevindrosendahl.javaannbench.bench;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Recall {

  interface Index {
    List<Integer> query(float[] vector, int k) throws IOException;
  }

  public record Results(double recall, Duration averageExecutionTime) {}

  public static Results test(Index index, List<float[]> queries, int k, List<List<Integer>> groundTruths)
      throws IOException {
    var numQueries = queries.size();
    var totalDuration = Duration.ZERO;
    var recalls = new ArrayList<Double>(queries.size());

    for (int i = 0; i < numQueries; i++) {
      var query = queries.get(i);

      var start = Instant.now();
      var results = index.query(query, k);
      var end = Instant.now();

      var groundTruth = groundTruths.get(i);
      var truePositives = groundTruth.stream().limit(k).filter(results::contains).count();
      var recall = (double) truePositives / k;
      recalls.add(recall);

      var duration = Duration.between(start, end);
      totalDuration = totalDuration.plus(duration);
    }

    var recall = recalls.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
    var averageExecutionTime = totalDuration.dividedBy(numQueries);
    return new Results(recall, averageExecutionTime);
  }

}
