package com.github.kevindrosendahl.javaannbench.index;

import com.github.kevindrosendahl.javaannbench.dataset.Dataset;
import com.github.kevindrosendahl.javaannbench.util.Bytes;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface Index extends AutoCloseable {

  String description();

  interface Builder extends Index {
    BuildSummary build() throws IOException;

    Bytes size() throws IOException;

    static Builder fromDescription(Dataset dataset, Path indexesPath, String description)
        throws IOException {
      var parameters = Builder.Parameters.parse(description);
      var datasetPath = indexesPath.resolve(dataset.description());
      Files.createDirectories(datasetPath);

      return switch (parameters.provider) {
        case "lucene" -> LuceneHnswIndex.Builder.create(
            datasetPath, dataset.train(), dataset.similarityFunction(), parameters);
        case "jvector" -> JvectorIndex.Builder.create(
            datasetPath, dataset.train(), dataset.similarityFunction(), parameters);
        default -> throw new RuntimeException("unknown index provider: " + parameters.type);
      };
    }

    record BuildSummary(Duration build, Duration commit) {}

    record Parameters(String provider, String type, Map<String, String> buildParameters) {

      public static Parameters parse(String description) {
        var parts = description.split("_");
        Preconditions.checkArgument(
            parts.length == 3, "unexpected build description format: %s", description);

        var provider = parts[0];
        var type = parts[1];
        var buildParametersString = parts[2];

        var buildParameters =
            Arrays.stream(buildParametersString.split("-"))
                .map(s -> s.split(":"))
                .peek(
                    p ->
                        Preconditions.checkArgument(
                            p.length == 2,
                            "unexpected build parameter description format: %s",
                            String.join("-", p)))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]));

        return new Parameters(provider, type, buildParameters);
      }
    }
  }

  interface Querier extends Index {

    List<Integer> query(float[] vector, int k) throws IOException;

    static Querier fromDescription(Dataset dataset, Path indexesPath, String description)
        throws IOException {
      var parameters = Parameters.parse(description);
      return switch (parameters.provider) {
        case "lucene" -> LuceneHnswIndex.Querier.create(
            indexesPath.resolve(dataset.description()), parameters);
        default -> throw new RuntimeException("unknown index provider: " + parameters.provider);
      };
    }

    record Parameters(
        String provider,
        String type,
        Map<String, String> buildParameters,
        Map<String, String> queryParameters) {

      public static Parameters parse(String description) {
        var parts = description.split("_");
        Preconditions.checkArgument(
            parts.length == 4, "unexpected query description format: %s", description);

        var provider = parts[0];
        var type = parts[1];
        var buildParametersString = parts[2];
        var queryParametersString = parts[3];

        var buildParameters =
            Arrays.stream(buildParametersString.split("-"))
                .map(s -> s.split(":"))
                .peek(
                    p ->
                        Preconditions.checkArgument(
                            p.length == 2,
                            "unexpected build parameter description format: %s",
                            String.join("-", p)))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]));

        var queryParameters =
            Arrays.stream(queryParametersString.split("-"))
                .map(s -> s.split(":"))
                .peek(
                    p ->
                        Preconditions.checkArgument(
                            p.length == 2,
                            "unexpected query parameter description format: %s",
                            String.join("-", p)))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]));

        return new Parameters(provider, type, buildParameters, queryParameters);
      }
    }
  }
}
