#include <iostream>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <liburing.h>
#include <stdlib.h>
#include <cassert>
#include <list>
#include <variant>
#include <unordered_map>

static const uint32_t BUFFER_COUNT = 64;
static const uint32_t BUFFER_SIZE = 1024 * 4 * 8;
static const uint32_t PAGE_SIZE = 1024 * 4;


struct Read {
  uint64_t starting_file_offset;
  uint32_t read_size;
  uint32_t buffer_index;
};

struct Write {
  uint64_t starting_file_offset;
  uint32_t write_size;
  uint32_t buffer_index;
};

struct StatX {

};

struct OpenAt {

};

using OutstandingOp = std::variant<Read, Write, StatX, OpenAt>;

// From https://www.bfilipek.com/2018/06/variant.html
template<class... Ts> struct overload : Ts... { using Ts::operator()...; };
template<class... Ts> overload(Ts...) -> overload<Ts...>;


int main() {
  struct io_uring ring;
  // see man io_uring_setup for what this does
  int ring_flags = IORING_SETUP_SQPOLL;
  auto ret = io_uring_queue_init(64, &ring, ring_flags);

  if (ret == -EPERM) {
    ring_flags = 0;
    perror("Failed to initalize uring with kernel queue thread.");
    ret = io_uring_queue_init(64, &ring, ring_flags);
  }

  if (ret) {
    perror("Failed initialize uring.");
    exit(1);
  }

  std::cout << "Uring initialized successfully. " << std::endl;
  // Even if we are not using writev, readv we need to have buffers in this format in order to do async I/O

  struct iovec *buffers = new struct iovec[BUFFER_COUNT];
  for (uint32_t i = 0; i < BUFFER_COUNT; i++) {
    buffers[i].iov_len = BUFFER_SIZE;
    if (posix_memalign(&buffers[i].iov_base, PAGE_SIZE, BUFFER_SIZE)) {
      std::cerr << "memory allocation failed" << std::endl;
      exit(1);
    }
  }

  auto buffer_register_err = io_uring_register_buffers(&ring, buffers, BUFFER_COUNT);
  if (buffer_register_err) {
    std::cerr << "Buffer registration failed. " << -buffer_register_err << " " << strerror(-buffer_register_err) << std::endl;
  }

  std::list<uint32_t> available_buffers;
  for (uint32_t i = 0; i < BUFFER_COUNT; i++) {
    available_buffers.push_back(i);
  }

  std::unordered_map<uint32_t, OutstandingOp> outstanding_ops;

  uint32_t next_op_id = 0;
  struct io_uring_sqe *submission_queue_entry = io_uring_get_sqe(&ring);
  outstanding_ops.try_emplace(next_op_id, OpenAt{});
  io_uring_prep_openat(submission_queue_entry, -1, "/tmp/stuff", O_RDONLY | O_DIRECT, 0);
  submission_queue_entry->user_data = next_op_id++;


  submission_queue_entry = io_uring_get_sqe(&ring);
  outstanding_ops.try_emplace(next_op_id, StatX{});
  struct statx statx_info;
  io_uring_prep_statx(submission_queue_entry, -1, "/tmp/stuff", 0, STATX_SIZE, &statx_info);
  submission_queue_entry->user_data = next_op_id++;

  submission_queue_entry = io_uring_get_sqe(&ring);
  outstanding_ops.try_emplace(next_op_id, OpenAt{});
  io_uring_prep_openat(submission_queue_entry, -1, "/tmp/dest", O_RDWR | O_CREAT | O_DIRECT, S_IRWXU);
  submission_queue_entry->user_data = next_op_id++;

  //TODO: what does this actually return?
  auto submit_error = io_uring_submit(&ring);
  if (submit_error != 3) {
    std::cerr << strerror(submit_error) << std::endl;
    exit(2);
  }

  bool got_statx = false;
  int src_file_descriptor = -1;
  int dest_file_descriptor = -1;
  uint64_t bytes_remaining = 0;

  // Buffers in this list contain data ready to write to destination, but don't yet have a slot in the out going
  // io_uring submission queue.  So this is another level of queuing.
  std::list<Write> pending_writes;

  while (!outstanding_ops.empty()) {
    struct io_uring_cqe *completion_queue_entry = 0;
    auto wait_return = io_uring_wait_cqe(&ring, &completion_queue_entry);
    if (wait_return) {
      std::cerr << "Completion queue wait error. " << std::endl;
      exit(2);
    }

    std::cout << "res : " << completion_queue_entry->res << " user data " << completion_queue_entry->user_data << std::endl;
    std::cout.flush();
    auto it = outstanding_ops.find(static_cast<uint32_t>(completion_queue_entry->user_data));
    assert(it != outstanding_ops.end());

    // The return value for everything in the completion queue is signed 32-bit int even if the underlying system call
    // is actually greater.  For example read returns ssize_t;  sizeof(ssize_t) is 8 bytes.
    if (completion_queue_entry->res < 0) {
      const char* op_name;
      std::visit(overload{
          [&op_name](const Read &r) { op_name = "read"; },
          [&op_name](const Write &w) { op_name = "write"; },
          [&op_name](const StatX &s) { op_name = "statx"; },
          [&op_name](const OpenAt &w) { op_name = "openat"; }
      }, it->second);
      std::cerr << "Failed" << strerror(-completion_queue_entry->res) << std::endl;
      exit(2);
    }


    std::visit(overload {
        [&](const Read& r) {
          // TODO: retry if there are fewer bytes than asked for.
          assert(completion_queue_entry->res == r.read_size);
          pending_writes.emplace_back(Write{r.starting_file_offset, r.read_size, r.buffer_index});
        },
        [&](const Write& w) {
          assert(completion_queue_entry->res == w.write_size);
          available_buffers.push_back(w.buffer_index);
        },
        [&](const StatX& x) {
          got_statx = true;
          bytes_remaining = statx_info.stx_size;
          std::cout << "Statx complete." << std::endl;
        },
        [&](const OpenAt& o) {
          if (completion_queue_entry->user_data == 0) {
            src_file_descriptor = completion_queue_entry->res;
          } else {
            dest_file_descriptor = completion_queue_entry->res;
          }
          std::cout << "OpenAt complete." << std::endl;
        }
    }, it->second);

    // not erasing with iterator here because outstanding_ops may have been modified.
    outstanding_ops.erase(static_cast<uint32_t>(completion_queue_entry->user_data));
    io_uring_cqe_seen(&ring, completion_queue_entry);

    if (src_file_descriptor == -1 || dest_file_descriptor == -1 || !got_statx) {
      // Not ready for reading or writing.  Actually we could be read to read but not write and start reading anyway, but that's another optimization
      continue;
    }

    uint32_t prepared_count = 0;
    while (!pending_writes.empty()) {
      struct io_uring_sqe *write_submission = io_uring_get_sqe(&ring);
      if (!write_submission) {
        break;
      }
      Write &write_op = pending_writes.front();
      //TODO: I think I can skip this step and setup all the writes ahead of time by chaining them to the reads
      io_uring_prep_write_fixed(write_submission,
                                dest_file_descriptor,
                                buffers[write_op.buffer_index].iov_base,
                                write_op.write_size,
                                write_op.starting_file_offset,
                                write_op.buffer_index);
      write_submission->user_data = next_op_id;
      outstanding_ops.try_emplace(next_op_id, std::move(write_op));
      pending_writes.pop_front();
      ++next_op_id;
      ++prepared_count;
    }

    if (prepared_count > 0) {
      auto submit_count = io_uring_submit(&ring);
      std::cout << "Submitted " << prepared_count << " writes." << std::endl;
      assert(submit_count == prepared_count);
    }

    if (bytes_remaining > 0 && !available_buffers.empty()) {
      prepared_count = 0;
      std::cout << "read_more" << std::endl;
      while (bytes_remaining && !available_buffers.empty()) {
        struct io_uring_sqe* read_submission = io_uring_get_sqe(&ring);
        if (!read_submission) {
          break;
        }
        uint32_t buffer_index = *available_buffers.begin();
        auto file_read_offset = statx_info.stx_size - bytes_remaining;
        uint32_t read_size = (bytes_remaining < BUFFER_SIZE) ? statx_info.stx_size - bytes_remaining : BUFFER_SIZE;
        io_uring_prep_read_fixed(read_submission, src_file_descriptor, buffers[buffer_index].iov_base, read_size, file_read_offset, buffer_index);
        read_submission->user_data = next_op_id;
        available_buffers.pop_front();
        outstanding_ops.try_emplace(next_op_id, Read{file_read_offset, read_size, buffer_index});
        bytes_remaining -= read_size;
        ++next_op_id;
        ++prepared_count;
        std::cout << "read submission user_data " << read_submission->user_data << std::endl;
      }

      if (prepared_count > 0) {
        auto submit_count = io_uring_submit(&ring);
        std::cout << "submit_count " << submit_count << " prepared_count " << prepared_count << std::endl;
        assert(submit_count == prepared_count);
      }
    }

  }

  io_uring_unregister_buffers(&ring);
}
