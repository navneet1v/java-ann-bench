package com.github.kevindrosendahl.javaannbench.dataset;

import com.github.kevindrosendahl.javaannbench.dataset.AnnBenchmarkDatasets.Datasets;
import java.nio.file.Path;

public class Main {

  public static void main(String[] args) throws Exception {
    Path workingDirectory = Path.of(System.getProperty("user.dir"));
    Path annBenchmarkDataSetDirectory = workingDirectory.resolve("datasets")
        .resolve("ann-benchmarks");
    var glove100 = AnnBenchmarkDatasets.load(Datasets.GLOVE_100, annBenchmarkDataSetDirectory);
    System.out.println("glove100.test().size() = " + glove100.test().size());
  }
}