#ifndef WRAPPED_URING_H
#define WRAPPED_URING_H

#include <liburing.h>
#include <stdint.h>

struct wrapped_io_uring {
  struct io_uring *wrapped;
  struct io_uring_cqe *cqe;
  int fd;
};

struct wrapped_result {
  uint64_t user_data;
  int res;
};

struct wrapped_io_uring *wrapped_io_uring_init_from_path(char *path,
                                                         unsigned entries);

struct wrapped_io_uring *wrapped_io_uring_init_from_fd(int fd,
                                                       unsigned entries);

void wrapped_io_uring_prep_read(struct wrapped_io_uring *ring,
                                uint64_t user_data, void *buf, unsigned nbytes,
                                off_t offset);

void wrapped_io_uring_submit_requests(struct wrapped_io_uring *ring);

struct wrapped_result *
wrapped_io_uring_wait_for_request(struct wrapped_io_uring *ring);

void wrapped_io_uring_complete_request(struct wrapped_io_uring *ring,
                                       struct wrapped_result *result);

void wrapped_io_uring_close_ring(struct wrapped_io_uring *ring);

#endif // WRAPPED_URING_H
