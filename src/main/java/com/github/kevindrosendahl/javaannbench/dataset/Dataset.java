package com.github.kevindrosendahl.javaannbench.dataset;

import com.github.kevindrosendahl.javaannbench.dataset.AnnBenchmarkDatasets.Datasets;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public record Dataset(SimilarityFunction similarityFunction, int dimensions, List<float[]> train,
                      List<float[]> test, List<List<Integer>> groundTruth) {

  public static Dataset fromDescription(Path datasetPath, String description)
      throws IOException, InterruptedException {
    var parts = description.split("/");
    Preconditions.checkArgument(parts.length == 2,
        "expect dataset name of form <collection>/<name>");
    Preconditions.checkArgument(parts[0].equals("ann-benchmarks"),
        "unexpected dataset collection name: %s", parts[0]);

    var name = parts[1];
    var annBenchmarksDataset = Arrays.stream(Datasets.values())
        .filter(annDataset -> annDataset.description.equals(name)).findFirst();
    Preconditions.checkArgument(annBenchmarksDataset.isPresent(), "unexpected dataset name: %s",
        name);

    return AnnBenchmarkDatasets.load(annBenchmarksDataset.get(),
        datasetPath.resolve("ann-benchmarks"));
  }

  static Dataset fromCsv(int dimensions, SimilarityFunction similarityFunction, Path path)
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
