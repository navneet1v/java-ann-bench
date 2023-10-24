package com.github.kevindrosendahl.javaannbench.display;

public interface Progress extends AutoCloseable {

  void inc();

  void inc(int value);

  void incTo(int value);
}
