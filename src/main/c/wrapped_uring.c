#include "wrapped_uring.h"
#include <fcntl.h>
#include <liburing.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define FILE_PATH "/java-ann-bench/datasets/glove-100-angular/test.fvecs"
#define BUFFER_SIZE 400

int main() {
  struct wrapped_io_uring *ring = wrapped_io_uring_init_from_path(FILE_PATH, 8);

  // Allocate buffers for read
  char *buf1 = malloc(BUFFER_SIZE);
  char *buf2 = malloc(BUFFER_SIZE);

  wrapped_io_uring_prep_read(ring, 0, buf1, BUFFER_SIZE, 0);
  wrapped_io_uring_prep_read(ring, 1, buf2, BUFFER_SIZE, BUFFER_SIZE * 13);

  wrapped_io_uring_submit_requests(ring);

  // Complete the requests
  for (int i = 0; i < 2; i++) {
    struct wrapped_result *result = wrapped_io_uring_wait_for_request(ring);
    if (result->res < 0) {
      fprintf(stderr, "IO error: %s\n", strerror(-result->res));
    } else {
      printf("Read %d bytes as vector: [", result->res);
      int numFloats = result->res / sizeof(float);
      float *floatBuf = i == 0 ? (float *)buf1 : (float *)buf2;
      for (int j = 0; j < numFloats; j++) {
        printf("%f", floatBuf[j]);
        if (j < numFloats - 1) {
          printf(", ");
        }
      }
      printf("]\n");
    }
    wrapped_io_uring_complete_request(ring, result);
  }

  wrapped_io_uring_close_ring(ring);
  free(buf1);
  free(buf2);

  return 0;
}

struct wrapped_io_uring *wrapped_io_uring_init_from_path(char *path,
                                                         unsigned entries) {
  int fd = open(FILE_PATH, O_RDONLY);
  if (fd < 0) {
    perror("Failed to open file");
    return NULL;
  }

  struct wrapped_io_uring *ring = wrapped_io_uring_init_from_fd(fd, entries);
  if (ring == NULL) {
    close(fd);
    return NULL;
  }

  return ring;
}

struct wrapped_io_uring *wrapped_io_uring_init_from_fd(int fd,
                                                       unsigned entries) {
  struct io_uring *ring = malloc(sizeof(struct io_uring));
  if (ring == NULL) {
    perror("Failed to allocate memory for io_uring");
    return NULL;
  }

  io_uring_queue_init(entries, ring, 0);

  struct wrapped_io_uring *wrapped = malloc(sizeof(struct wrapped_io_uring));
  if (wrapped == NULL) {
    perror("Failed to allocate memory for wrapped_io_uring");
    free(ring);
    return NULL;
  }

  wrapped->wrapped = ring;
  wrapped->fd = fd;
  return wrapped;
}

void wrapped_io_uring_prep_read(struct wrapped_io_uring *ring,
                                uint64_t user_data, void *buf, unsigned nbytes,
                                off_t offset) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(ring->wrapped);
  io_uring_sqe_set_data(sqe, (void *)user_data);
  io_uring_prep_read(sqe, ring->fd, buf, nbytes, offset);
}

void wrapped_io_uring_submit_requests(struct wrapped_io_uring *ring) {
  io_uring_submit(ring->wrapped);
}

struct wrapped_result *
wrapped_io_uring_wait_for_request(struct wrapped_io_uring *ring) {
  int res = io_uring_wait_cqe(ring->wrapped, &(ring->cqe));
  if (res < 0) {
    perror("Failed to wait for completion");
    return NULL;
  }

  struct wrapped_result *result = malloc(sizeof(struct wrapped_result));
  if (result == NULL) {
    perror("Failed to allocate memory for wrapped_result");
    return NULL;
  }
  result->res = ring->cqe->res;
  result->user_data = (uint64_t)io_uring_cqe_get_data(ring->cqe);
  return result;
}

void wrapped_io_uring_complete_request(struct wrapped_io_uring *ring,
                                       struct wrapped_result *result) {
  io_uring_cqe_seen(ring->wrapped, ring->cqe);
  free(result);
}

void wrapped_io_uring_close_ring(struct wrapped_io_uring *ring) {
//  close(ring->fd);
  io_uring_queue_exit(ring->wrapped);
  free(ring);
}
