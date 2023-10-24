package com.github.kevindrosendahl.javaannbench.dataset;

import java.io.IOException;
import java.nio.file.Path;

public class Tarball {

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
}
