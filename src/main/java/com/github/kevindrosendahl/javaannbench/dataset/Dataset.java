package com.github.kevindrosendahl.javaannbench.dataset;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public record Dataset(SimilarityFunction similarityFunction, int dimensions, List<float[]> train,
                      List<float[]> test) {

  public static Dataset fromCsv(int dimensions, SimilarityFunction similarityFunction, Path path)
      throws IOException {
    var trainPath = path.resolve("train.csv");
    Preconditions.checkArgument(trainPath.toFile().exists());
    var train = CsvVectorLoader.load(trainPath, dimensions);

    var testPath = path.resolve("test.csv");
    Preconditions.checkArgument(testPath.toFile().exists());
    var test = CsvVectorLoader.load(testPath, dimensions);

    return new Dataset(similarityFunction, dimensions, train, test);
  }

}
