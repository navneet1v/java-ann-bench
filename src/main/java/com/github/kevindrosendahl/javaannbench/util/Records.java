package com.github.kevindrosendahl.javaannbench.util;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.util.Map;

public class Records {

  @SuppressWarnings("unchecked")
  public static <T> T fromMap(Map<String, String> parameters, Class<T> clazz, String description) {
    var fields = clazz.getRecordComponents();
    Constructor<T> constructor = (Constructor<T>) clazz.getDeclaredConstructors()[0];
    Preconditions.checkArgument(
        constructor.getParameterCount() == parameters.size(),
        "unexpected number of parameters when parsing %s. expected %s, got %s",
        description,
        constructor.getParameterCount(),
        parameters.size());

    var args = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      var component = fields[i];

      var name = component.getName();
      Preconditions.checkArgument(parameters.containsKey(name), "must specify %s", name);

      var value = parameters.get(name);
      var parsed = parse(value, component.getType());
      args[i] = parsed;
    }

    constructor.setAccessible(true);
    try {
      return constructor.newInstance(args);
    } catch (Exception e) {
      throw new RuntimeException("failed parsing record", e);
    }
  }

  private static Object parse(String value, Class<?> type) {
    if (type == int.class) {
      return Integer.parseInt(value);
    }

    if (type == float.class) {
      return Float.parseFloat(value);
    }

    if (type == boolean.class) {
      return Boolean.parseBoolean(value);
    }

    throw new RuntimeException("unsupported class type: " + type);
  }
}
