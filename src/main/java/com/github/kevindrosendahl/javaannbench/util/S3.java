package com.github.kevindrosendahl.javaannbench.util;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.internal.TransferManagerFactory;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

public class S3 {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3.class);
  private static final Region REGION = Region.US_EAST_1;
  private static final String BUCKET = "kevin-vector-search";
  private static final String DOWNLOAD_PROGRESS_FILE_SUFFIX = "-download-progress.yaml";
  private static final String UPLOAD_PROGRESS_FILE_SUFFIX = "-upload-progress.yaml";

  public static void downloadAndExtract(Path destination, Path object)
      throws IOException, InterruptedException {
    download(destination, object);
    extract(destination, object.getFileName().toString());
  }

  public static void archiveAndUpload(Path directory, Path objectPrefix)
      throws IOException, InterruptedException, ExecutionException {
    archive(directory);
    upload(directory, objectPrefix);
  }

  private static void download(Path directory, Path object) throws IOException {
    var file = object.getFileName().toString();
    var progress = DownloadProgress.load(directory, file);
    if (progress.downloaded) {
      LOGGER.debug("{} already downloaded to {}, skipping", file, directory);
      return;
    }

    if (directory.resolve(file).toFile().exists()) {
      LOGGER.warn(
          "{} exists at {} but did not finish downloading. wiping and starting download over",
          file,
          directory);
      Files.delete(directory.resolve(file));
    }

    LOGGER.info("downloading {} to {}", file, directory);
    Files.createDirectories(directory);

    try (S3AsyncClient s3 =
        S3AsyncClient.crtBuilder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build()) {
      try (var transferManager = new TransferManagerFactory.DefaultBuilder().s3Client(s3).build()) {
        var request =
            DownloadFileRequest.builder()
                .getObjectRequest(b -> b.bucket(BUCKET).key(object.toString()))
                .addTransferListener(LoggingTransferListener.create())
                .destination(directory.resolve(file))
                .build();

        transferManager.downloadFile(request).completionFuture().join();
      }
    }

    new DownloadProgress(true, false).store(directory, file);
  }

  private static void upload(Path directory, Path prefix) throws IOException {
    var file =
        directory.getParent().resolve(directory.getFileName().toString() + Tarball.GZIPPED_FORMAT);

    try (S3AsyncClient s3 =
        S3AsyncClient.crtBuilder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build()) {
      try (var transferManager = new TransferManagerFactory.DefaultBuilder().s3Client(s3).build()) {
        var request =
            UploadFileRequest.builder()
                .putObjectRequest(
                    b -> b.bucket(BUCKET).key(prefix.resolve(file.getFileName()).toString()))
                .addTransferListener(LoggingTransferListener.create())
                .source(file)
                .build();

        transferManager.uploadFile(request).completionFuture().join();
      }
    }

    new UploadProgress(true, true).store(directory.getParent(), directory.getFileName().toString());
  }

  private static void extract(Path directory, String file)
      throws IOException, InterruptedException {
    Preconditions.checkArgument(
        file.endsWith(Tarball.GZIPPED_FORMAT), "file to extract %s is not a gzipped tarball", file);
    var extractedFile = file.substring(0, file.length() - Tarball.GZIPPED_FORMAT.length());

    var progress = DownloadProgress.load(directory, file);
    Preconditions.checkArgument(progress.downloaded);
    if (progress.extracted) {
      LOGGER.debug("{} already extracted in {}, skipping", file, directory);
      return;
    }

    if (directory.resolve(extractedFile).toFile().exists()) {
      LOGGER.info(
          "{} exists in {} but did not finish extracting. wiping and starting extraction over",
          extractedFile,
          directory);
      FileUtils.deleteDirectory(directory.resolve(extractedFile).toFile());
    }

    Tarball.extractGzipped(directory.resolve(file));

    new DownloadProgress(true, true).store(directory, file);
    LOGGER.info("finished extracting {} to {}", file, directory);
  }

  private static void archive(Path directory) throws IOException, InterruptedException {
    Preconditions.checkArgument(
        Files.isDirectory(directory), "path to archive %s is not directory", directory);

    var directoryName = directory.getFileName().toString();
    var parent = directory.getParent();

    var progress = UploadProgress.load(parent, directoryName);
    if (progress.archived) {
      LOGGER.debug("{} already archived in {}, skipping", directoryName, parent);
      return;
    }

    var archivedName = directoryName + Tarball.GZIPPED_FORMAT;
    if (parent.resolve(archivedName).toFile().exists()) {
      LOGGER.info(
          "{} exists in {} but did not finish archiving. wiping and starting extraction over",
          archivedName,
          directory);
      FileUtils.delete(parent.resolve(archivedName).toFile());
    }

    Tarball.archiveGzipped(directory);

    new UploadProgress(true, false).store(parent, directoryName);
    LOGGER.info("finished archiving {} to {}", directory, parent.resolve(archivedName));
  }

  private record DownloadProgress(boolean downloaded, boolean extracted) {

    static DownloadProgress load(Path directory, String file) throws IOException {
      var progressFile = directory.resolve(file + DOWNLOAD_PROGRESS_FILE_SUFFIX).toFile();
      if (!progressFile.exists()) {
        return new DownloadProgress(false, false);
      }

      return Yaml.fromYaml(progressFile, DownloadProgress.class);
    }

    void store(Path directory, String file) throws IOException {
      var progressFile = directory.resolve(file + DOWNLOAD_PROGRESS_FILE_SUFFIX).toFile();
      if (!progressFile.exists()) {
        Files.createFile(progressFile.toPath());
      }

      Yaml.writeYaml(this, progressFile);
    }
  }

  private record UploadProgress(boolean archived, boolean uploaded) {

    static UploadProgress load(Path directory, String file) throws IOException {
      var progressFile = directory.resolve(file + UPLOAD_PROGRESS_FILE_SUFFIX).toFile();
      if (!progressFile.exists()) {
        return new UploadProgress(false, false);
      }

      return Yaml.fromYaml(progressFile, UploadProgress.class);
    }

    void store(Path directory, String file) throws IOException {
      var progressFile = directory.resolve(file + UPLOAD_PROGRESS_FILE_SUFFIX).toFile();
      if (!progressFile.exists()) {
        Files.createFile(progressFile.toPath());
      }

      Yaml.writeYaml(this, progressFile);
    }
  }
}
