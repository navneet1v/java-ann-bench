package com.github.kevindrosendahl.javaannbench.dataset;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;

public class FlatIntWriter {

  public static void writeVectors(Path path, List<Integer> integers) throws IOException {
    try (var fos = new FileOutputStream(path.toFile());
        var channel = fos.getChannel()) {

      int size = integers.size();
      var buffer = ByteBuffer.allocate(size * Integer.BYTES);
      buffer.order(ByteOrder.LITTLE_ENDIAN);

      for (var integer : integers) {
        buffer.putInt(integer);
      }

      buffer.flip();
      channel.write(buffer);
    }
  }
}
