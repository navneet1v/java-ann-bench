package com.github.kevindrosendahl.javaannbench.util.iouring;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class IoUring implements Closeable {

  public static class Result {
    private final MemorySegment inner;
    public int res;
    public long id;

    private Result(MemorySegment inner, int res, long id) {
      this.inner = inner;
      this.res = res;
      this.id = id;
    }
  }

  private final MemorySegment ring;
  private final AtomicLong counter = new AtomicLong();
  private final Map<Long, CompletableFuture<Void>> futures = new HashMap<>();

  private IoUring(MemorySegment ring) {
    this.ring = ring;
  }

  public static IoUring create(Path file, int entries) {
    MemorySegment ring;
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment path = arena.allocateUtf8String(file.toString());
      ring = WrappedLib.initRing(path, entries);
    }
    return new IoUring(ring);
  }

  public CompletableFuture<Void> prepare(MemorySegment buffer, int numbytes, long offset) {
    long id = counter.getAndIncrement();
    WrappedLib.prepRead(ring, id, buffer, numbytes, offset);
    CompletableFuture<Void> future = new CompletableFuture<>();
    futures.put(id, future);
    return future;
  }

  public void submit() {
    WrappedLib.submitRequests(ring);
  }

  public void awaitAll() {
    while (true) {
      boolean empty = await();
      if (empty) {
        return;
      }
    }
  }

  public boolean await() {
    MemorySegment result = WrappedLib.waitForRequest(ring);
    int res = (int) WrappedLib.WRAPPED_RESULT_RES_HANDLE.get(result);
    long id = (long) WrappedLib.WRAPPED_RESULT_USER_DATA_HANDLE.get(result);
    WrappedLib.completeRequest(ring, result);

    CompletableFuture<Void> future = futures.remove(id);
    if (res < 0) {
      future.completeExceptionally(new IOException("error reading file, errno: " + res));
    } else {
      future.complete(null);
    }

    return futures.isEmpty();
  }

  public void completeRequest(Result result) {
    WrappedLib.completeRequest(ring, result.inner);
  }

  @Override
  public void close() {
    WrappedLib.closeRing(ring);
  }
}
