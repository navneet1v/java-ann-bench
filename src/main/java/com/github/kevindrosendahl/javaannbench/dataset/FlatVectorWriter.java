package com.github.kevindrosendahl.javaannbench.dataset;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;

public class FlatVectorWriter {

  public static void writeVectors(Path path, RandomAccessVectorValues<float[]> vectors)
      throws IOException {
    try (var fos = new FileOutputStream(path.toFile());
        var channel = fos.getChannel()) {

      int size = vectors.size();
      int dimension = vectors.dimension();
      int chunkSize = 1024;
      long position = 0;

      var buffer = ByteBuffer.allocate(chunkSize * dimension * Float.BYTES);
      buffer.order(ByteOrder.LITTLE_ENDIAN);

      for (int i = 0; i < size; i++) {
        var vector = vectors.vectorValue(i);
        for (float f : vector) {
          buffer.putFloat(f);
        }

        if (!buffer.hasRemaining()) {
          buffer.flip();
          channel.position(position);
          channel.write(buffer);
          position += buffer.limit();
          buffer.clear();
        }
      }

      // Handle any remaining data
      if (buffer.position() > 0) {
        buffer.flip();
        channel.position(position);
        channel.write(buffer);
      }
    }
  }

  public static void writeVectors(Path path, List<float[]> vectors) throws IOException {
    try (var fos = new FileOutputStream(path.toFile());
        var channel = fos.getChannel()) {

      int size = vectors.size();
      var buffer = ByteBuffer.allocate(size * vectors.get(0).length * Float.BYTES);
      buffer.order(ByteOrder.LITTLE_ENDIAN);

      for (var vector : vectors) {
        for (float f : vector) {
          buffer.putFloat(f);
        }
      }

      buffer.flip();
      channel.write(buffer);
    }
  }
}
