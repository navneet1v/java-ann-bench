package com.github.kevindrosendahl.javaannbench.dataset;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public record Dataset(SimilarityFunction similarityFunction, List<float[]> train,
                      List<float[]> test) {

  public static Dataset fromCsv(SimilarityFunction similarityFunction, Path path)
      throws IOException {
    var trainPath = path.resolve("train.csv");
    Preconditions.checkArgument(trainPath.toFile().exists());
    var train = CsvVectorLoader.load(trainPath);

    var testPath = path.resolve("test.csv");
    Preconditions.checkArgument(testPath.toFile().exists());
    var test = CsvVectorLoader.load(testPath);

    return new Dataset(similarityFunction, train, test);
  }

}
