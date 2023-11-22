#include "wrapped_uring.h"
#include <fcntl.h>
#include <liburing.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define FILE_PATH "/home/ubuntu/java-ann-bench/datasets/glove-100-angular/test.fvecs"
#define BUFFER_SIZE 400

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

  int ret = io_uring_queue_init(entries, ring, 0);
  if (ret < 0) {
    perror("Failed to initialize ring");
    free(ring);
    return NULL;
  }

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
  io_uring_prep_read(sqe, ring->fd, buf, nbytes, offset);
  io_uring_sqe_set_data(sqe, (void *)user_data);
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
