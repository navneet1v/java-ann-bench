#ifndef WRAPPED_URING_H
#define WRAPPED_URING_H

#include <liburing.h>
#include <stdint.h>

struct wrapped_io_uring {
  struct io_uring *wrapped;
  int fd;
  struct io_uring_cqe *cqe;
};

struct wrapped_result {
  int res;
  uint64_t user_data;
};

struct wrapped_io_uring *init_ring(char *path, unsigned entries);

void prep_read(struct wrapped_io_uring *ring, uint64_t user_data, void *buf,
               unsigned nbytes, off_t offset);

void submit_requests(struct wrapped_io_uring *ring);

struct wrapped_result *wait_for_request(struct wrapped_io_uring *ring);

void complete_request(struct wrapped_io_uring *ring);

void close_ring(struct wrapped_io_uring *ring);

#endif // WRAPPED_URING_H
