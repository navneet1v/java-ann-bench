package com.github.kevindrosendahl.javaannbench.dataset;

import com.github.kevindrosendahl.javaannbench.dataset.AnnBenchmarkDatasets.AnnBenchmarkDataset;
import com.github.kevindrosendahl.javaannbench.util.MMapRandomAccessVectorValues;
import com.google.common.base.Preconditions;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public record Dataset(
    String description,
    SimilarityFunction similarityFunction,
    int dimensions,
    RandomAccessVectorValues<float[]> train,
    RandomAccessVectorValues<float[]> test,
    List<List<Integer>> groundTruth) {

  public static Dataset fromDescription(Path datasetPath, String description)
      throws IOException, InterruptedException {
    var parts = description.split("_");
    Preconditions.checkArgument(parts.length == 2, "unexpected dataset format: %s", description);
    Preconditions.checkArgument(
        parts[0].equals("ann-benchmarks"), "unexpected dataset collection name: %s", parts[0]);

    var name = parts[1];
    var annBenchmarksDataset =
        Arrays.stream(AnnBenchmarkDataset.values())
            .filter(annDataset -> annDataset.description.equals(name))
            .findFirst();
    Preconditions.checkArgument(
        annBenchmarksDataset.isPresent(), "unexpected dataset name: %s", name);

    return AnnBenchmarkDatasets.load(
        annBenchmarksDataset.get(), datasetPath.resolve("ann-benchmarks"));
  }

  static Dataset load(
      String description,
      int numTrainVectors,
      int numTestVectors,
      int dimensions,
      SimilarityFunction similarityFunction,
      Path path)
      throws IOException {
    var trainPath = path.resolve("train.fvecs");
    Preconditions.checkArgument(trainPath.toFile().exists());
    var train = new MMapRandomAccessVectorValues(trainPath, numTrainVectors, dimensions);

    var testPath = path.resolve("test.fvecs");
    Preconditions.checkArgument(testPath.toFile().exists());
    var test = new MMapRandomAccessVectorValues(testPath, numTestVectors, dimensions);

    var neighborsPath = path.resolve("neighbors.csv");
    Preconditions.checkArgument(neighborsPath.toFile().exists());
    var neighbors = CsvVectorLoader.loadGroundTruth(neighborsPath);

    return new Dataset(description, similarityFunction, dimensions, train, test, neighbors);
  }
}
