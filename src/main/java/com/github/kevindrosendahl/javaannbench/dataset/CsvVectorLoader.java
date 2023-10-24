package com.github.kevindrosendahl.javaannbench.dataset;

import com.github.kevindrosendahl.javaannbench.display.ProgressBar;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

public class CsvVectorLoader {

  public static List<float[]> loadVectors(String description, Path path, int dimensions)
      throws IOException {
    var rows = Files.lines(path).count();

    try (var progress =
        ProgressBar.create(String.format("loading %s vectors", description), (int) rows)) {
      var vectors = new ArrayList<float[]>();

      try (var reader = Files.newBufferedReader(path);
          var parser = new CSVParser(reader, CSVFormat.DEFAULT)) {
        for (var record : parser) {
          Preconditions.checkArgument(
              record.size() == dimensions,
              "row's dimensions %s does not match expected %s",
              record.size(),
              dimensions);
          var vector = new float[record.size()];

          var dimension = 0;
          for (var value : record) {
            vector[dimension++] = Float.parseFloat(value);
          }

          vectors.add(vector);
          progress.inc();
        }
      }

      return vectors;
    }
  }

  public static List<List<Integer>> loadGroundTruth(Path path) throws IOException {
    var rows = Files.lines(path).count();

    try (var progress = ProgressBar.create("loading ground truth vectors", (int) rows)) {
      var groundTruths = new ArrayList<List<Integer>>();

      try (var reader = Files.newBufferedReader(path);
          var parser = new CSVParser(reader, CSVFormat.DEFAULT)) {
        for (var record : parser) {
          var groundTruth = new ArrayList<Integer>(record.size());
          for (var value : record) {
            groundTruth.add(Integer.parseInt(value));
          }

          groundTruths.add(groundTruth);
          progress.inc();
        }

        return groundTruths;
      }
    }
  }
}
