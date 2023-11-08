package com.github.kevindrosendahl.javaannbench.util;

public class Exceptions {

  public interface CheckedRunnable<E extends Exception> {
    void run() throws E;
  }

  public static <E extends Exception> void wrap(CheckedRunnable<E> runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
