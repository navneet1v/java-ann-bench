package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.util.Yaml;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public record QuerySpec(
    String dataset,
    String provider,
    String type,
    Map<String, String> buildParameters,
    Map<String, String> queryParameters,
    int k,
    RuntimeConfiguration runtimeConfiguration) {

  public record RuntimeConfiguration(String dockerMemory, String heapSize, int queryThreads) {}

  public static QuerySpec load(Path path) throws Exception {
    return Yaml.fromYaml(path.toFile(), QuerySpec.class);
  }

  public String toString() {
    var buildParams =
        buildParameters.entrySet().stream()
            .sorted(Entry.comparingByKey())
            .map(entry -> "%s:%s")
            .collect(Collectors.joining("-"));

    var queryParams =
        queryParameters.entrySet().stream()
            .sorted(Entry.comparingByKey())
            .map(entry -> "%s:%s")
            .collect(Collectors.joining("-"));

    var runtimeParams =
        String.format(
            "dockerMemory:%s-heapSize:%s-queryThreads:%s",
            runtimeConfiguration.dockerMemory,
            runtimeConfiguration.heapSize,
            runtimeConfiguration.queryThreads);

    return String.format(
        "%s_%s_%s_%s_%s_%s_%s",
        dataset, provider, type, buildParams, queryParams, k, runtimeParams);
  }
}
