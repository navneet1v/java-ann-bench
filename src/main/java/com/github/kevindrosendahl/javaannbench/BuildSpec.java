package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.util.Yaml;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public record BuildSpec(String dataset, String provider, String type, Map<String, String> build) {

  public static BuildSpec load(Path path) throws Exception {
    return Yaml.fromYaml(path.toFile(), BuildSpec.class);
  }

  public String toString() {
    return String.format("%s_%s_%s_%s", dataset, provider, type, buildString());
  }

  public String buildString() {
    return build.entrySet().stream()
        .sorted(Entry.comparingByKey())
        .map(entry -> String.format("%s:%s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining("-"));
  }
}
