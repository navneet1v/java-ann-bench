package com.github.kevindrosendahl.javaannbench.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class Yaml {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  public static String toYaml(Object value) {
    try {
      return YAML_MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static void writeYaml(Object value, File file) throws IOException {
    try {
      var yaml = toYaml(value);
      Files.writeString(file.toPath(), yaml);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T fromYaml(String yaml, Class<T> clazz) {
    try {
      return YAML_MAPPER.readValue(yaml, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public  static <T> T fromYaml(File yaml, Class<T> clazz) throws IOException {
    try {
      return YAML_MAPPER.readValue(yaml, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

}
