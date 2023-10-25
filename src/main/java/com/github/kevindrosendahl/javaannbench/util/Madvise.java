package com.github.kevindrosendahl.javaannbench.util;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

public class Madvise {
  public enum Advice {
    NORMAL(0),
    RANDOM(1),
    SEQUENTIAL(2),
    WILLNEED(3),
    DONTNEED(4);

    private final int code;

    Advice(int code) {
      this.code = code;
    }
  }

  private static final MethodHandle MADVISE;
  private static final MethodHandle ERRNO;

  static {
    var linker = Linker.nativeLinker();
    var stdlib = linker.defaultLookup();

    MADVISE =
        linker.downcallHandle(
            stdlib.find("madvise").get(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT));

    ERRNO =
        linker.downcallHandle(
            stdlib.find("errno").get(), FunctionDescriptor.of(ValueLayout.JAVA_INT));
  }

  public static void advise(MemorySegment segment, long length, Advice advice) {
    int result;
    try {
      result = (int) MADVISE.invokeExact(segment, length, advice.code);
    } catch (Throwable t) {
      throw new RuntimeException("caught exception invoking madvise", t);
    }

    if (result == 0) {
      return;
    }

    var errno = getErrno();
    throw new RuntimeException("got error calling madvise, errno " + errno);
  }

  private static int getErrno() {
    try {
      return (int) ERRNO.invokeExact();
    } catch (Throwable t) {
      throw new RuntimeException("caught exception invoking errno", t);
    }
  }
}
