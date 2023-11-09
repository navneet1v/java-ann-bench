package com.github.kevindrosendahl.javaannbench;

import com.github.kevindrosendahl.javaannbench.util.Yaml;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public record BuildSpec(
    String dataset, String provider, String type, Map<String, String> buildParameters) {

  public static BuildSpec load(Path path) throws Exception {
    return Yaml.fromYaml(path.toFile(), BuildSpec.class);
  }

  public String toString() {
    var buildParams =
        buildParameters.entrySet().stream()
            .sorted(Entry.comparingByKey())
            .map(entry -> "%s:%s")
            .collect(Collectors.joining("-"));

    return String.format("%s_%s_%s_%s", dataset, provider, type, buildParams);
  }
}
