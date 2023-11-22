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
    Map<String, String> runtime) {

  public record RuntimeConfiguration(
      String systemMemory, String heapSize, int queryThreads, boolean jfr) {}

  public static QuerySpec load(Path path) throws Exception {
    return Yaml.fromYaml(path.toFile(), QuerySpec.class);
  }

  public String toString() {
    return String.format(
        "%s_%s_%s_%s_%s_%s_%s",
        dataset, provider, type, buildString(), queryString(), k, runtimeString());
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

  public String runtimeString() {
    return runtime.entrySet().stream()
        .sorted(Entry.comparingByKey())
        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining("-"));
  }
}
