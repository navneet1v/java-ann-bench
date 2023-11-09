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
    Map<String, String> build,
    Map<String, String> query,
    int k,
    RuntimeConfiguration runtime) {

  public record RuntimeConfiguration(String systemMemory, String heapSize, int queryThreads) {}

  public static QuerySpec load(Path path) throws Exception {
    return Yaml.fromYaml(path.toFile(), QuerySpec.class);
  }

  public String toString() {
    var runtimeParams =
        String.format(
            "systemMemory:%s-heapSize:%s-queryThreads:%s",
            runtime.systemMemory, runtime.heapSize, runtime.queryThreads);

    return String.format(
        "%s_%s_%s_%s_%s_%s_%s",
        dataset, provider, type, buildString(), queryString(), k, runtimeParams);
  }

  public String buildString() {
    return build.entrySet().stream()
        .sorted(Entry.comparingByKey())
        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining("-"));
  }

  public String queryString() {
    return query.entrySet().stream()
        .sorted(Entry.comparingByKey())
        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining("-"));
  }
}
