package com.github.kevindrosendahl.javaannbench.util;

import static java.lang.foreign.MemoryLayout.*;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;

import java.io.IOException;
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

public class IOUring {
  private static final MemoryLayout SQE_LAYOUT =
      structLayout(
              JAVA_BYTE.withName("opcode"), // __u8 opcode
              JAVA_BYTE.withName("flags"), // __u8 flags
              JAVA_SHORT.withName("ioprio"), // __u16 ioprio
              JAVA_INT.withName("fd"), // __s32 fd
              JAVA_LONG.withName("off_or_addr2"), // union { __u64 off; __u64 addr2; }
              JAVA_LONG.withName(
                  "addr_or_splice_off_in"), // union { __u64 addr; __u64 splice_off_in; }
              JAVA_INT.withName("len"), // __u32 len
              JAVA_INT.withName(
                  "rw_flags_or_other_flags"), // union { __kernel_rwf_t rw_flags; __u32 ...; }
              JAVA_LONG.withName("user_data"), // __u64 user_data
              unionLayout(
                      structLayout(
                              JAVA_SHORT.withName(
                                  "buf_index_or_group"), // union { __u16 buf_index; __u16
                              // buf_group; }
                              JAVA_SHORT.withName("personality"), // __u16 personality
                              JAVA_INT.withName("splice_fd_in") // __s32 splice_fd_in
                              )
                          .withName("inner_struct"),
                      JAVA_LONG.withName("pad2") // __u64 __pad2[3]
                      )
                  .withName("union"))
          .withName("io_uring_sqe");

  private static final VarHandle IO_URING_SQE_USER_DATA_HANDLE =
      SQE_LAYOUT.varHandle(PathElement.groupElement("user_data"));

  private static final MemoryLayout IO_URING_CQE_LAYOUT =
      MemoryLayout.structLayout(
              JAVA_LONG.withName("user_data"), // __u64 user_data
              JAVA_INT.withName("res"), // __s32 res
              JAVA_INT.withName("flags") // __u32 flags
              )
          .withName("io_uring_cqe");

  private static final VarHandle IO_URING_CQE_USER_DATA_HANDLE =
      IO_URING_CQE_LAYOUT.varHandle(PathElement.groupElement("user_data"));

  private static final VarHandle IO_URING_CQE_RES_HANDLE =
      IO_URING_CQE_LAYOUT.varHandle(PathElement.groupElement("res"));

  private static final MemoryLayout IO_URING_SQ_LAYOUT =
      MemoryLayout.structLayout(
              JAVA_LONG.withName("khead"), // unsigned *khead
              JAVA_LONG.withName("ktail"), // unsigned *ktail
              JAVA_LONG.withName("kring_mask"), // unsigned *kring_mask
              JAVA_LONG.withName("kring_entries"), // unsigned *kring_entries
              JAVA_LONG.withName("kflags"), // unsigned *kflags
              JAVA_LONG.withName("kdropped"), // unsigned *kdropped
              JAVA_LONG.withName("array"), // unsigned *array
              JAVA_LONG.withName("sqes"), // struct io_uring_sqe *sqes
              JAVA_INT.withName("sqe_head"), // unsigned sqe_head
              JAVA_INT.withName("sqe_tail"), // unsigned sqe_tail
              JAVA_LONG.withName("ring_sz"), // size_t ring_sz
              JAVA_LONG.withName("ring_ptr") // void *ring_ptr
              )
          .withName("io_uring_sq");

  private static final MemoryLayout IO_URING_CQ_LAYOUT =
      MemoryLayout.structLayout(
              JAVA_LONG.withName("khead"), // unsigned *khead
              JAVA_LONG.withName("ktail"), // unsigned *ktail
              JAVA_LONG.withName("kring_mask"), // unsigned *kring_mask
              JAVA_LONG.withName("kring_entries"), // unsigned *kring_entries
              JAVA_LONG.withName("koverflow"), // unsigned *koverflow
              JAVA_LONG.withName("cqes"), // struct io_uring_cqe *cqes
              JAVA_LONG.withName("ring_sz"), // size_t ring_sz
              JAVA_LONG.withName("ring_ptr") // void *ring_ptr
              )
          .withName("io_uring_cq");

  private static final MemoryLayout IO_URING_LAYOUT =
      MemoryLayout.structLayout(
              IO_URING_SQ_LAYOUT.withName("sq"), // struct io_uring_sq sq
              IO_URING_CQ_LAYOUT.withName("cq"), // struct io_uring_cq cq
              JAVA_INT.withName("flags"), // unsigned flags
              JAVA_INT.withName("ring_fd") // int ring_fd
              )
          .withName("io_uring");

  private enum OpenFlags {
    O_RDONLY(0),
    O_WRONLY(1),
    O_RDWR(2);

    private final int code;

    OpenFlags(int code) {
      this.code = code;
    }
  }

  private static final String IO_URING_QUEUE_INIT_NAME = "io_uring_queue_init";
  private static final MethodHandle IO_URING_QUEUE_INIT;
  private static final String IO_URING_GET_SQE_NAME = "io_uring_get_sqe";
  private static final MethodHandle IO_URING_GET_SQE;
  private static final String IO_URING_PREP_READ_NAME = "io_uring_prep_read";
  private static final MethodHandle IO_URING_PREP_READ;
  private static final String IO_URING_SUBMIT_NAME = "io_uring_submit";
  private static final MethodHandle IO_URING_SUBMIT;
  private static final String IO_URING_WAIT_CQE_NAME = "io_uring_wait_cqe";
  private static final MethodHandle IO_URING_WAIT_CQE;
  private static final String IO_URING_CQE_SEEN_NAME = "io_uring_cqe_seen";
  private static final MethodHandle IO_URING_CQE_SEEN;
  private static final String IO_URING_QUEUE_EXIT_NAME = "io_uring_queue_exit";
  private static final MethodHandle IO_URING_QUEUE_EXIT;
  private static final MethodHandle OPEN;
  private static final MethodHandle CLOSE;
  private static final MethodHandle ERRNO;

  static {
    Arena global = Arena.global();
    SymbolLookup uringLookup = SymbolLookup.libraryLookup("liburing", global);
    var linker = Linker.nativeLinker();
    var stdlib = linker.defaultLookup();

    IO_URING_QUEUE_INIT =
        linker.downcallHandle(
            uringLookup.find(IO_URING_QUEUE_INIT_NAME).get(),
            FunctionDescriptor.of(
                JAVA_INT, /* returns int */
                JAVA_INT, /* unsigned entries */
                ADDRESS, /* struct io_uring *ring */
                JAVA_INT /* unsigned flags */));

    IO_URING_GET_SQE =
        linker.downcallHandle(
            uringLookup.find(IO_URING_GET_SQE_NAME).get(),
            FunctionDescriptor.of(
                ADDRESS, /* returns struct io_uring_sqe* */ ADDRESS /* struct io_uring *ring */));

    IO_URING_PREP_READ =
        linker.downcallHandle(
            uringLookup.find(IO_URING_PREP_READ_NAME).get(),
            FunctionDescriptor.ofVoid(
                ADDRESS, /* struct io_uring_sqe *sqe */
                JAVA_INT, /* int fd */
                ADDRESS, /* void *buf */
                JAVA_INT, /* unsigned nbytes */
                JAVA_LONG /* off_t offset */));

    IO_URING_SUBMIT =
        linker.downcallHandle(
            uringLookup.find(IO_URING_SUBMIT_NAME).get(),
            FunctionDescriptor.of(JAVA_INT, /* returns int */ ADDRESS /* struct io_uring *ring */));

    IO_URING_WAIT_CQE =
        linker.downcallHandle(
            uringLookup.find(IO_URING_WAIT_CQE_NAME).get(),
            FunctionDescriptor.of(
                JAVA_INT, /* returns int */
                ADDRESS, /* struct io_uring *ring */
                ADDRESS /* struct io_uring_cqe **cqe_ptr */));

    IO_URING_CQE_SEEN =
        linker.downcallHandle(
            uringLookup.find(IO_URING_CQE_SEEN_NAME).get(),
            FunctionDescriptor.ofVoid(
                ADDRESS, /* struct io_uring *ring */ ADDRESS /* struct io_uring_cqe *cqe */));

    IO_URING_QUEUE_EXIT =
        linker.downcallHandle(
            uringLookup.find(IO_URING_QUEUE_EXIT_NAME).get(),
            FunctionDescriptor.ofVoid(ADDRESS /* struct io_uring *ring */));

    OPEN =
        linker.downcallHandle(
            stdlib.find("open").get(),
            FunctionDescriptor.of(
                JAVA_INT, /* returns int */
                ADDRESS, /* const char *pathname */
                JAVA_INT /* int flags */));

    CLOSE =
        linker.downcallHandle(
            stdlib.find("close").get(),
            FunctionDescriptor.of(JAVA_INT, /* returns int */ JAVA_INT /* int fd */));

    ERRNO = linker.downcallHandle(stdlib.find("errno").get(), FunctionDescriptor.of(JAVA_INT));
  }

  public static void main(String[] args) throws Exception {
    var workingDirectory = Path.of(System.getProperty("user.dir"));
    var gloveTestPath = workingDirectory.resolve("datasets/glove-100-angular/test.fvecs");

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pathname = arena.allocateUtf8String(gloveTestPath.toString());
      int fd = open(pathname, OpenFlags.O_RDONLY.code);

      MemorySegment ring = arena.allocate(IO_URING_LAYOUT);
      queueInit(8, ring, 0);

      long bufferSize = 100 * Float.BYTES;
      MemorySegment buf1 = arena.allocate(bufferSize);
      MemorySegment buf2 = arena.allocate(bufferSize);

      MemorySegment sqe = getSqe(ring);
      IO_URING_SQE_USER_DATA_HANDLE.set(sqe, 0L);
      prepRead(sqe, fd, buf1, (int) bufferSize, 0);

      sqe = getSqe(ring);
      IO_URING_SQE_USER_DATA_HANDLE.set(sqe, 1L);
      prepRead(sqe, fd, buf2, (int) bufferSize, bufferSize * 13);

      submit(ring);

      MemorySegment cqe = arena.allocate(IO_URING_CQE_LAYOUT);
      MemorySegment cqePtr = arena.allocate(ADDRESS.byteSize());
      cqePtr.set(ADDRESS, 0, cqe);
      for (int i = 0; i < 2; i++) {
        waitCqe(ring, cqePtr);

        int res = (int) IO_URING_CQE_RES_HANDLE.get(cqe);
        if (res < 0) {
          throw new IOException("error reading file, errno: " + res);
        }

        long userData = (long) IO_URING_CQE_USER_DATA_HANDLE.get(cqe);
        System.out.println("finished read " + userData);

        MemorySegment buf = userData == 0 ? buf1 : buf2;
        System.out.println("buf = " + Arrays.toString(buf.toArray(JAVA_FLOAT)));

        cqeSeen(ring, cqe);
      }

      close(fd);
      queueExit(ring);
    }
  }

  private static int open(MemorySegment pathname, int flags) {
    int result;
    try {
      result = (int) OPEN.invokeExact(pathname, flags);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString("open"));
    }

    if (result > 0) {
      return result;
    }

    processErrnoResult(result, "open");
    return -1;
  }

  private static void close(int fd) {
    int result;
    try {
      result = (int) CLOSE.invokeExact(fd);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString("open"));
    }

    processErrnoResult(result, "open");
  }

  private static void queueInit(int entries, MemorySegment ring, int flags) {
    int result;
    try {
      result = (int) IO_URING_QUEUE_INIT.invokeExact(entries, ring, 0);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(IO_URING_QUEUE_INIT_NAME));
    }

    processErrnoResult(result, IO_URING_QUEUE_INIT_NAME);
  }

  private static MemorySegment getSqe(MemorySegment ring) {
    long address;
    try {
      address = (long) IO_URING_GET_SQE.invokeExact(ring);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(IO_URING_GET_SQE_NAME));
    }

    if (address == 0) {
      throw new RuntimeException("ring is full");
    }

    MemorySegment sqe = MemorySegment.ofAddress(address);
    return sqe.reinterpret(SQE_LAYOUT.byteSize());
  }

  private static void prepRead(
      MemorySegment sqe, int fd, MemorySegment buf, int nbytes, long offset) {
    try {
      IO_URING_PREP_READ.invokeExact(sqe, fd, buf, nbytes, offset);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(IO_URING_PREP_READ_NAME));
    }
  }

  private static int submit(MemorySegment ring) {
    try {
      return (int) IO_URING_SUBMIT.invokeExact(ring);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(IO_URING_PREP_READ_NAME));
    }
  }

  private static void waitCqe(MemorySegment ring, MemorySegment cqe) {
    int result;
    try {
      result = (int) IO_URING_WAIT_CQE.invokeExact(ring, cqe);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(IO_URING_WAIT_CQE_NAME));
    }

    processErrnoResult(result, IO_URING_WAIT_CQE_NAME);
  }

  private static void cqeSeen(MemorySegment ring, MemorySegment cqe) {
    try {
      IO_URING_CQE_SEEN.invokeExact(ring, cqe);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(IO_URING_CQE_SEEN_NAME));
    }
  }

  private static void queueExit(MemorySegment ring) {
    try {
      IO_URING_QUEUE_EXIT.invokeExact(ring);
    } catch (Throwable t) {
      throw new RuntimeException(invokeErrorString(IO_URING_QUEUE_EXIT_NAME));
    }
  }

  private static String invokeErrorString(String name) {
    return "caught excpetion invoking " + name;
  }

  private static void processErrnoResult(int result, String name) {
    if (result == 0) {
      return;
    }

    var errno = getErrno();
    throw new RuntimeException("got error calling " + name + ", errno " + errno);
  }

  private static int getErrno() {
    try {
      return (int) ERRNO.invokeExact();
    } catch (Throwable t) {
      throw new RuntimeException("caught exception invoking errno", t);
    }
  }
}
