package com.github.kevindrosendahl.javaannbench.dataset;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class IVecs {

  public static List<List<Integer>> load(Path path, int size, int dimensions) throws IOException {
    try (var fis = new FileInputStream(path.toFile());
        var channel = fis.getChannel()) {

      var buffer = ByteBuffer.allocate(size * dimensions * Integer.BYTES);
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      channel.read(buffer);
      buffer.flip();

      List<List<Integer>> ints = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        List<Integer> innerList = new ArrayList<>(dimensions);
        for (int j = 0; j < dimensions; j++) {
          innerList.add(buffer.getInt());
        }
        ints.add(innerList);
      }

      return ints;
    }
  }

  public static void write(Path path, List<List<Integer>> ints) throws IOException {
    try (var fos = new FileOutputStream(path.toFile());
        var channel = fos.getChannel()) {

      int size = ints.size();
      int dimensions = ints.get(0).size();

      var buffer = ByteBuffer.allocate(size * dimensions * Integer.BYTES);
      buffer.order(ByteOrder.LITTLE_ENDIAN);

      for (var outer : ints) {
        for (var inner : outer) {
          buffer.putInt(inner);
        }
      }

      buffer.flip();
      channel.write(buffer);
    }
  }
}
