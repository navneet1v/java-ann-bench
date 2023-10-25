package com.github.kevindrosendahl.javaannbench.dataset;

import com.github.kevindrosendahl.javaannbench.util.S3;
import com.github.kevindrosendahl.javaannbench.util.Tarball;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;

public enum Datasets {
  GIST_960("gist-960-euclidean", 1000000, 1000, 960, SimilarityFunction.EUCLIDEAN),
  GLOVE_100("glove-100-angular", 1183514, 10000, 100, SimilarityFunction.COSINE),
  GLOVE_25("glove-25-angular", 1183514, 10000, 25, SimilarityFunction.COSINE),
  MNIST_784("mnist-784-euclidean", 60000, 10000, 784, SimilarityFunction.EUCLIDEAN),
  NYTIMES_256("nytimes-256-angular", 290000, 10000, 256, SimilarityFunction.COSINE),
  SIFT_128("sift-128-euclidean", 1000000, 10000, 128, SimilarityFunction.EUCLIDEAN);

  public final String name;
  public final int numTrainVectors;
  public final int numTestVectors;
  public final int dimensions;
  public final SimilarityFunction similarityFunction;

  Datasets(
      String name,
      int numTrainVectors,
      int numTestVectors,
      int dimensions,
      SimilarityFunction similarityFunction) {
    this.name = name;
    this.numTrainVectors = numTrainVectors;
    this.numTestVectors = numTestVectors;
    this.dimensions = dimensions;
    this.similarityFunction = similarityFunction;
  }

  public static Dataset load(Path datasetsPath, String name)
      throws IOException, InterruptedException {
    var description =
        switch (name) {
          case "gist-960-euclidean" -> GIST_960;
          case "glove-100-angular" -> GLOVE_100;
          case "glove-25-angular" -> GLOVE_25;
          case "mnist-784-euclidean" -> MNIST_784;
          case "nytimes-256-angular" -> NYTIMES_256;
          case "sift-128-euclidean" -> SIFT_128;
          default -> throw new RuntimeException("unknown dataset " + name);
        };

    S3.downloadAndExtract(datasetsPath, Path.of("datasets").resolve(name + Tarball.GZIPPED_FORMAT));
    var datasetPath = datasetsPath.resolve(name);

    var trainPath = datasetPath.resolve("train.fvecs");
    Preconditions.checkArgument(trainPath.toFile().exists());
    var train = FVecs.mmap(trainPath, description.numTrainVectors, description.dimensions);

    var testPath = datasetPath.resolve("test.fvecs");
    Preconditions.checkArgument(testPath.toFile().exists());
    var test = FVecs.mmap(testPath, description.numTestVectors, description.dimensions);

    var neighborsPath = datasetPath.resolve("neighbors.ivecs");
    Preconditions.checkArgument(neighborsPath.toFile().exists());
    var neighbors = IVecs.load(neighborsPath, description.numTestVectors, 100);

    return new Dataset(
        name, description.similarityFunction, description.dimensions, train, test, neighbors);
  }
}
