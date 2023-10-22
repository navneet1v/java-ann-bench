package com.github.kevindrosendahl.javaannbench.dataset;

import com.google.common.base.Preconditions;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class AnnBenchmarkDatasets {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnnBenchmarkDatasets.class);
  private static final String PROGRESS = "progress.properties";
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
    return Dataset.fromCsv(dataset.dimensions, dataset.similarityFunction, datasetDir);
  }

  private static void ensure(Path directory) throws IOException, InterruptedException {
    download(directory);
    unzip(directory);
  }

  private static void download(Path directory) throws IOException {
    var progress = Progress.load(directory);
    if (progress.downloaded) {
      LOGGER.info("ann-datasets.tar.gz already downloaded to {}, skipping", directory);
      return;
    }

    if (directory.resolve(KEY).toFile().exists()) {
      LOGGER.info(
          "ann-datasets.tar.gz exists at {} but did not finish downloading. wiping and starting download over",
          directory);
      Files.delete(directory.resolve(KEY));
    }

    LOGGER.info("downloading ann-datasets.tar.gz to {}", directory);
    Files.createDirectories(directory);

    Region region = Region.US_EAST_1;
    try (S3Client s3 = S3Client.builder().region(region)
        .credentialsProvider(DefaultCredentialsProvider.create()).build()) {

      GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(BUCKET).key(KEY)
          .build();

      s3.getObject(getObjectRequest, ResponseTransformer.toFile(directory.resolve(KEY)));
    }

    new Progress(true, false).store(directory);
    LOGGER.info("finished downloading ann-datasets.tar.gz to {}", directory);
  }

  private static void unzip(Path directory) throws IOException, InterruptedException {
    var progress = Progress.load(directory);
    Preconditions.checkArgument(progress.downloaded);
    if (progress.extracted) {
      LOGGER.info("ann-datasets.tar.gz already extracted in {}, skipping", directory);
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

    private final static String DOWNLOADED = "downloaded";
    private final static String EXTRACTED = "extracted";

    static Progress load(Path directory) throws IOException {
      var file = directory.resolve(PROGRESS).toFile();
      if (!file.exists()) {
        return new Progress(false, false);
      }

      try (var input = new FileInputStream(file)) {
        var properties = new Properties();
        properties.load(input);
        return new Progress(
            Boolean.parseBoolean(properties.getProperty(DOWNLOADED, Boolean.FALSE.toString())),
            Boolean.parseBoolean(properties.getProperty(EXTRACTED, Boolean.FALSE.toString())));
      }
    }

    void store(Path directory) throws IOException {
      var file = directory.resolve(PROGRESS).toFile();
      if (!file.exists()) {
        Files.createFile(file.toPath());
      }

      try (var output = new FileOutputStream(file)) {
        var properties = new Properties();
        properties.setProperty(DOWNLOADED, Boolean.toString(this.downloaded));
        properties.setProperty(EXTRACTED, Boolean.toString(this.extracted));
        properties.store(output, null);
      }
    }
  }
}
