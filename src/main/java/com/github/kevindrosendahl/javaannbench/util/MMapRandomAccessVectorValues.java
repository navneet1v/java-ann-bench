package com.github.kevindrosendahl.javaannbench.util;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;

public class MMapRandomAccessVectorValues implements RandomAccessVectorValues<float[]> {
  private final MemorySegment segment;
  private final int size;
  private final int dimension;

  public MMapRandomAccessVectorValues(Path path, int size, int dimension) throws IOException {
    try (var channel = FileChannel.open(path)) {
      this.segment =
          channel.map(MapMode.READ_ONLY, 0, (long) size * dimension * Float.BYTES, Arena.global());
    }
    this.size = size;
    this.dimension = dimension;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public float[] vectorValue(int targetOrd) {
    if (targetOrd < 0 || targetOrd >= size) {
      throw new IllegalArgumentException("Invalid ordinal");
    }

    float[] result = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      result[i] =
          segment.getAtIndex(ValueLayout.JAVA_FLOAT_UNALIGNED, (long) targetOrd * dimension + i);
    }

    return result;
  }

  @Override
  public boolean isValueShared() {
    return false;
  }

  @Override
  public RandomAccessVectorValues<float[]> copy() {
    return this;
  }

  public void advise(Madvise.Advice advice) {
    Madvise.advise(this.segment, this.segment.byteSize(), advice);
  }
}
