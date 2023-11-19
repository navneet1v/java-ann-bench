package com.github.kevindrosendahl.javaannbench.util.iouring;

import static java.lang.foreign.MemoryLayout.*;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.Arrays;

public class WrappedLib {
  private static final MemoryLayout WRAPPED_IO_URING =
      structLayout(ADDRESS.withName("wrapped"), ADDRESS.withName("cqe"), JAVA_INT.withName("fd"));

  private static final MemoryLayout WRAPPED_RESULT =
      structLayout(JAVA_LONG.withName("user_data"), JAVA_INT.withName("res"));

  static final VarHandle WRAPPED_RESULT_RES_HANDLE =
      WRAPPED_RESULT.varHandle(PathElement.groupElement("res"));
  static final VarHandle WRAPPED_RESULT_USER_DATA_HANDLE =
      WRAPPED_RESULT.varHandle(PathElement.groupElement("user_data"));

  private static final String INIT_RING_FROM_PATH = "wrapped_io_uring_init_from_path";
  private static final MethodHandle INIT_RING_FROM_PATH_HANDLE;

  private static final String INIT_RING_FROM_FD = "wrapped_io_uring_init_from_fd";
  private static final MethodHandle INIT_RING_FROM_FD_HANDLE;

  private static final String PREP_READ = "wrapped_io_uring_prep_read";
  private static final MethodHandle PREP_READ_HANDLE;

  private static final String SUBMIT_REQUESTS = "wrapped_io_uring_submit_requests";
  private static final MethodHandle SUBMIT_REQUESTS_HANDLE;

  private static final String WAIT_FOR_REQUESTS = "wrapped_io_uring_wait_for_request";
  private static final MethodHandle WAIT_FOR_REQUESTS_HANDLE;

  private static final String COMPLETE_REQUEST = "wrapped_io_uring_complete_request";
  private static final MethodHandle COMPLETE_REQUEST_HANDLE;

  private static final String CLOSE_RING = "wrapped_io_uring_close_ring";
  private static final MethodHandle CLOSE_RING_HANDLE;

  static {
    Arena global = Arena.global();
    SymbolLookup uringLookup =
        SymbolLookup.libraryLookup(
            Path.of("/java-ann-bench/src/main/c/libwrappeduring.so"), global);
    var linker = Linker.nativeLinker();

    INIT_RING_FROM_PATH_HANDLE =
        linker.downcallHandle(
            uringLookup.find(INIT_RING_FROM_PATH).get(),
            FunctionDescriptor.of(
                ADDRESS, /* returns *wrapped_io_uring */
                ADDRESS, /* char *path */
                JAVA_INT /* unsigned entries */));

    INIT_RING_FROM_FD_HANDLE =
        linker.downcallHandle(
            uringLookup.find(INIT_RING_FROM_PATH).get(),
            FunctionDescriptor.of(
                ADDRESS, /* returns *wrapped_io_uring */
                JAVA_INT, /* int fd */
                JAVA_INT /* unsigned entries */));

    PREP_READ_HANDLE =
        linker.downcallHandle(
            uringLookup.find(PREP_READ).get(),
            FunctionDescriptor.ofVoid(
                ADDRESS, /* wrapped_io_uring *ring */
                JAVA_LONG, /* uint64_t user_data */
                ADDRESS, /* void *buf */
                JAVA_INT, /* unsigned nbytes */
                JAVA_LONG /* off_t offset */));

    SUBMIT_REQUESTS_HANDLE =
        linker.downcallHandle(
            uringLookup.find(SUBMIT_REQUESTS).get(),
            FunctionDescriptor.ofVoid(ADDRESS /* wrapped_io_uring *ring */));

    WAIT_FOR_REQUESTS_HANDLE =
        linker.downcallHandle(
            uringLookup.find(WAIT_FOR_REQUESTS).get(),
            FunctionDescriptor.of(
                ADDRESS, /* returns wrapped_result* */ ADDRESS /* wrapped_io_uring *ring */));

    COMPLETE_REQUEST_HANDLE =
        linker.downcallHandle(
            uringLookup.find(COMPLETE_REQUEST).get(),
            FunctionDescriptor.ofVoid(
                ADDRESS, /* wrapped_io_uring *ring */ ADDRESS /* wrapped_result *result */));

    CLOSE_RING_HANDLE =
        linker.downcallHandle(
            uringLookup.find(CLOSE_RING).get(),
            FunctionDescriptor.ofVoid(ADDRESS /* wrapped_io_uring *ring */));
  }

  public static void main(String[] args) throws Exception {
    var workingDirectory = Path.of(System.getProperty("user.dir"));
    var gloveTestPath = workingDirectory.resolve("datasets/glove-100-angular/test.fvecs");

    try (IoUring.FileFactory factory = IoUring.factory(gloveTestPath)) {
      try (IoUring uring = factory.create(8)) {
        try (Arena arena = Arena.ofConfined()) {
          long bufferSize = 100 * Float.BYTES;
          MemorySegment buf1 = arena.allocate(bufferSize);
          MemorySegment buf2 = arena.allocate(bufferSize);

          uring
              .prepare(buf1, (int) bufferSize, 0)
              .thenRun(
                  () -> {
                    System.out.println("finished read 0");
                    System.out.println("buf = " + Arrays.toString(buf1.toArray(JAVA_FLOAT)));
                  });
          uring
              .prepare(buf2, (int) bufferSize, bufferSize * 13)
              .thenRun(
                  () -> {
                    System.out.println("finished read 1");
                    System.out.println("buf = " + Arrays.toString(buf2.toArray(JAVA_FLOAT)));
                  });
          uring.submit();
          uring.awaitAll();
        }
      }
    }
  }

  static MemorySegment initRing(MemorySegment path, int entries) {
    MemorySegment uninterpreted;
    try {
      uninterpreted = (MemorySegment) INIT_RING_FROM_PATH_HANDLE.invokeExact(path, entries);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(INIT_RING_FROM_PATH), t);
    }

    if (uninterpreted == null) {
      throw new RuntimeException("ring is full");
    }

    return uninterpreted.reinterpret(WRAPPED_IO_URING.byteSize());
  }

  static MemorySegment initRing(int fd, int entries) {
    MemorySegment uninterpreted;
    try {
      uninterpreted = (MemorySegment) INIT_RING_FROM_FD_HANDLE.invokeExact(fd, entries);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(INIT_RING_FROM_PATH), t);
    }

    if (uninterpreted == null) {
      throw new RuntimeException("ring is full");
    }

    return uninterpreted.reinterpret(WRAPPED_IO_URING.byteSize());
  }

  static void prepRead(
      MemorySegment ring, long userData, MemorySegment buf, int nbytes, long offset) {
    try {
      PREP_READ_HANDLE.invokeExact(ring, userData, buf, nbytes, offset);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(PREP_READ), t);
    }
  }

  static void submitRequests(MemorySegment ring) {
    try {
      SUBMIT_REQUESTS_HANDLE.invokeExact(ring);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(SUBMIT_REQUESTS), t);
    }
  }

  static MemorySegment waitForRequest(MemorySegment ring) {
    MemorySegment uninterpreted;
    try {
      uninterpreted = (MemorySegment) WAIT_FOR_REQUESTS_HANDLE.invokeExact(ring);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(WAIT_FOR_REQUESTS), t);
    }

    if (uninterpreted == null) {
      throw new RuntimeException("error waiting for request");
    }

    return uninterpreted.reinterpret(WRAPPED_RESULT.byteSize());
  }

  static void completeRequest(MemorySegment ring, MemorySegment result) {
    try {
      COMPLETE_REQUEST_HANDLE.invokeExact(ring, result);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(COMPLETE_REQUEST), t);
    }
  }

  static void closeRing(MemorySegment ring) {
    try {
      CLOSE_RING_HANDLE.invokeExact(ring);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(CLOSE_RING), t);
    }
  }

  private static String invokeErrorString(String name) {
    return "caught excpetion invoking " + name;
  }
}
