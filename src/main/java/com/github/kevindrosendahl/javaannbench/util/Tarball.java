package com.github.kevindrosendahl.javaannbench.util;

import java.io.IOException;
import java.nio.file.Path;

public class Tarball {

  public static final String GZIPPED_FORMAT = ".tar.gz";

  public static void extractGzipped(Path tarball) throws IOException, InterruptedException {
    var process =
        new ProcessBuilder("tar", "xzvf", tarball.toString())
            .directory(tarball.getParent().toFile())
            .inheritIO()
            .start();
    var exit = process.waitFor();
    if (exit != 0) {
      throw new RuntimeException("failed to extract");
    }
  }

  public static void archiveGzipped(Path directory) throws IOException, InterruptedException {
    Path tarballPath = directory.getParent().resolve(directory.getFileName() + GZIPPED_FORMAT);
    var process =
        new ProcessBuilder(
                "tar", "czvf", tarballPath.toString(), directory.getFileName().toString())
            .directory(directory.getParent().toFile())
            .inheritIO()
            .start();
    var exit = process.waitFor();
    if (exit != 0) {
      throw new RuntimeException("failed to archive");
    }
  }
}
