package com.github.kevindrosendahl.javaannbench.dataset;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public record Dataset(SimilarityFunction similarityFunction, int dimensions, List<float[]> train,
                      List<float[]> test, List<List<Integer>> groundTruth) {

  public static Dataset fromCsv(int dimensions, SimilarityFunction similarityFunction, Path path)
      throws IOException {
    var trainPath = path.resolve("train.csv");
    Preconditions.checkArgument(trainPath.toFile().exists());
    var train = CsvVectorLoader.loadVectors(trainPath, dimensions);

    var testPath = path.resolve("test.csv");
    Preconditions.checkArgument(testPath.toFile().exists());
    var test = CsvVectorLoader.loadVectors(testPath, dimensions);

    var neighborsPath = path.resolve("neighbors.csv");
    Preconditions.checkArgument(neighborsPath.toFile().exists());
    var neighbors = CsvVectorLoader.loadGroundTruth(neighborsPath);

    return new Dataset(similarityFunction, dimensions, train, test, neighbors);
  }

}
