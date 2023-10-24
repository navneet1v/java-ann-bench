package com.github.kevindrosendahl.javaannbench.dataset;

import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.github.kevindrosendahl.javaannbench.util.Yaml;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

public class AnnBenchmarkDatasets {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnnBenchmarkDatasets.class);
  private static final String PROGRESS = "progress.yaml";
  private static final String BUCKET = "kevin-vector-search";
  private static final String KEY = "ann-datasets.tar.gz";
  private static final String EXTRACTED = "ann-datasets";

  public enum Datasets {
    GIST_960(960, SimilarityFunction.EUCLIDEAN, "gist-960-euclidean"),
    GLOVE_100(100, SimilarityFunction.COSINE, "glove-100-angular"),
    GLOVE_25(25, SimilarityFunction.COSINE, "glove-25-angular"),
    MNIST_784(784, SimilarityFunction.EUCLIDEAN, "mnist-784-euclidean"),
    NYTIMES_256(256, SimilarityFunction.COSINE, "nytimes-256-angular"),
    SIFT_128(128, SimilarityFunction.COSINE, "sift-128-angular");

    public final int dimensions;
    public final SimilarityFunction similarityFunction;
    public final String description;

    Datasets(int dimensions, SimilarityFunction similarityFunction, String description) {
      this.dimensions = dimensions;
      this.similarityFunction = similarityFunction;
      this.description = description;
    }
  }

  public static Dataset load(Datasets dataset, Path directory)
      throws IOException, InterruptedException {
    ensure(directory);
    var datasetDir = directory.resolve(EXTRACTED).resolve(Path.of(dataset.description));
    return Dataset.fromCsv(
        "ann-benchmarks_" + dataset.description,
        dataset.dimensions,
        dataset.similarityFunction,
        datasetDir);
  }

  private static void ensure(Path directory) throws IOException, InterruptedException {
    download(directory);
    unzip(directory);
  }

  private static void download(Path directory) throws IOException {
    var progress = Progress.load(directory);
    if (progress.downloaded) {
      LOGGER.debug("ann-datasets.tar.gz already downloaded to {}, skipping", directory);
      return;
    }

    if (directory.resolve(KEY).toFile().exists()) {
      LOGGER.warn(
          "ann-datasets.tar.gz exists at {} but did not finish downloading. wiping and starting download over",
          directory);
      Files.delete(directory.resolve(KEY));
    }

    Files.createDirectories(directory);

    Region region = Region.US_EAST_1;
    try (S3Client s3 =
        S3Client.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build()) {

      HeadObjectRequest headObjectRequest =
          HeadObjectRequest.builder().bucket(BUCKET).key(KEY).build();

      HeadObjectResponse headObjectResponse = s3.headObject(headObjectRequest);
      var totalSize = Bytes.ofBytes(headObjectResponse.contentLength());

      var filePath = directory.resolve(KEY);
      var downloadFuture =
          CompletableFuture.runAsync(
              () -> {
                var getObjectRequest = GetObjectRequest.builder().bucket(BUCKET).key(KEY).build();
                s3.getObject(getObjectRequest, ResponseTransformer.toFile(filePath));
              });

      try (var progressBar =
          ProgressBar.create(
              "downloading ann-datasets.tar.gz", (int) totalSize.toMebi(), "MiB", 1)) {
        while (!downloadFuture.isDone()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          if (Files.exists(filePath)) {
            var size = (int) Bytes.ofBytes(Files.size(filePath)).toMebi();
            progressBar.incTo(size);
          }
        }
      }
    }

    new Progress(true, false).store(directory);
    LOGGER.info("finished downloading ann-datasets.tar.gz to {}", directory);
  }

  private static void unzip(Path directory) throws IOException, InterruptedException {
    var progress = Progress.load(directory);
    Preconditions.checkArgument(progress.downloaded);
    if (progress.extracted) {
      LOGGER.debug("ann-datasets.tar.gz already extracted in {}, skipping", directory);
      return;
    }

    if (directory.resolve(EXTRACTED).toFile().exists()) {
      LOGGER.info(
          "ann-datasets exists at {} but did not finish extracting. wiping and starting extraction over",
          directory);
      FileUtils.deleteDirectory(directory.resolve(EXTRACTED).toFile());
    }

    Tarball.extractGzipped(directory.resolve(KEY));

    new Progress(true, true).store(directory);
    LOGGER.info("finished extracting ann-datasets.tar.gz to {}", directory);
  }

  private record Progress(boolean downloaded, boolean extracted) {

    static Progress load(Path directory) throws IOException {
      var file = directory.resolve(PROGRESS).toFile();
      if (!file.exists()) {
        return new Progress(false, false);
      }

      return Yaml.fromYaml(file, Progress.class);
    }

    void store(Path directory) throws IOException {
      var file = directory.resolve(PROGRESS).toFile();
      if (!file.exists()) {
        Files.createFile(file.toPath());
      }

      Yaml.writeYaml(this, file);
    }
  }
}
