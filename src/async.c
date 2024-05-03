#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "dfc/types.h"
#include "dfc/bloom_filter.h"
#include "dfc/dfc_util.h"
#include "dfc/sk_util.h"
#include "dfc/async.h"

void *async_dfc_send(void *arg) {
  SocketBuffer *sk_buf;
  ssize_t bytes_sent;

  sk_buf = (SocketBuffer *)arg;

  pthread_mutex_lock(&sk_buf->mutex);

  // send data
  if ((bytes_sent = dfc_send(sk_buf->sockfd, sk_buf->data, sk_buf->len_data)) !=
      sk_buf->len_data) {
    fprintf(stderr, "[ERROR] incomplete send\n");
  }

#ifdef DEBUG
  fprintf(stderr, "[INFO] sent %zd bytes over socket %d (expected=%zd)\n",
          bytes_sent, sk_buf->sockfd, sk_buf->len_data);
  fflush(stderr);
#endif

  pthread_mutex_unlock(&sk_buf->mutex);

  return NULL;
}

void *handle_get(void *arg) {
  DFCOperation *dfc_op = (DFCOperation *)arg;

  print_op(dfc_op);

  return NULL;
}

void *handle_list(void *arg) {
  (void)arg;
  fputs("[INFO] list thread\n", stderr);

  return NULL;
}

int handle_put(char *fname, int *sockfds, size_t n_servers) {
  unsigned int srv_alloc_start;
  char *file_content, *pair, **file_pieces;
  size_t srv_id, len_file, len_pair, len_hdr;
  size_t chunk_sizes[n_servers];
  pthread_t send_tids[n_servers];
  int ran_threads[n_servers];
  SocketBuffer sk_buf[n_servers];

  // determine if put can be done
  if (adjacent_failure(sockfds, n_servers)) {
    // not connected to enough servers
    for (size_t i = 0; i < n_servers; ++i) {
      if (sockfds[i] > 0 && close(sockfds[i]) == -1) {  // close connected sockets, if any
        perror("close");
      }
    }

    return -1;
  }

  srv_alloc_start = hash_djb2(fname) % n_servers;

  // read file (MAKE THIS ASYNCHRONOUS, JUST HAVE SEND THREAD WAIT TILL ITS DONE
  if ((file_content = read_file(fname, &len_file)) == NULL) {
    fprintf(stderr, "[ERROR] failed to read %s: %s\n", fname, strerror(errno));
    return -1;
  }

  // get chunk sizes
  get_chunk_sizes(len_file, n_servers, chunk_sizes);
  file_pieces = split_file(file_content, chunk_sizes, n_servers);

  // last chunk size will always be maximum
  if ((pair = alloc_buf(chunk_sizes[n_servers - 1] * 2)) == NULL) {
    fprintf(stderr, "[FATAL] out of memory\n");
    exit(EXIT_FAILURE);
  }

  for (size_t i = 0; i < n_servers; ++i) {
    srv_id = (srv_alloc_start + i) % n_servers;
    fprintf(stderr, "[INFO] selected server %zu\n", srv_id);

    len_pair =
        chunk_sizes[srv_id] + chunk_sizes[(srv_id + 1) % n_servers];

    if ((sk_buf[srv_id].data = alloc_buf(len_pair + sizeof(DFCHeader))) ==
        NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    if (sockfds[srv_id] == -1) {  // acceptable, decided beforehand
      continue;
    }

    pthread_mutex_init(&sk_buf[srv_id].mutex, NULL);
    pthread_mutex_lock(&sk_buf[srv_id].mutex);

    merge(file_pieces[srv_id], chunk_sizes[srv_id],
          file_pieces[(srv_id + 1) % n_servers],
          chunk_sizes[(srv_id + 1) % n_servers], pair);

    sk_buf[srv_id].sockfd = sockfds[srv_id];

    len_hdr = attach_hdr(
        sk_buf[srv_id].data, "put", fname,
        chunk_sizes[srv_id] + chunk_sizes[(srv_id + 1) % n_servers]);
    memcpy(sk_buf[srv_id].data + len_hdr, pair, len_pair);

    sk_buf[srv_id].len_data = len_hdr + len_pair;

    fprintf(stderr, "[INFO] sending pieces %zu, %zu of %s\n",
            srv_id, (srv_id + 1) % n_servers, fname);

    pthread_mutex_unlock(&sk_buf[srv_id].mutex);

    if (pthread_create(&send_tids[srv_id], NULL, async_dfc_send,
                       &sk_buf[srv_id])) {
      fprintf(stderr, "[%s] could not create thread %zu\n", __func__, i);
      exit(EXIT_FAILURE);
    }

    ran_threads[srv_id] = 1;
  }

  for (size_t i = 0; i < n_servers; ++i) {
    if (ran_threads[i] == 1) {
      pthread_join(send_tids[i], NULL);
    }
  }

  for (size_t i = 0; i < n_servers; ++i) {
    if (sk_buf[i].data != NULL) {
      free(sk_buf[i].data);
    }

    if (file_pieces[i] != NULL) {
      free(file_pieces[i]);
    }
  }

  if (file_pieces != NULL) {
    free(file_pieces);
  }

  if (file_content != NULL) {
    free(file_content);
  }

  if (pair != NULL) {
    free(pair);
  }

  return 0;
}

int handle_put2(DFCOperation *dfc_op) {
  unsigned int srv_alloc_start;
  size_t srv_id, len_file;
  char *file_content, *pair, **file_pieces;
  size_t len_pair;
  int sockfds[dfc_op->n_servers];
  size_t chunk_sizes[dfc_op->n_servers], len_hdr;
  pthread_t send_tids[dfc_op->n_servers];
  int ran_threads[dfc_op->n_servers];
  SocketBuffer sk_buf[dfc_op->n_servers];

  // determine if put can be done
  if (adjacent_failure(sockfds, dfc_op->n_servers)) {
    // not connected to enough servers
    for (size_t i = 0; i < dfc_op->n_servers; ++i) {
      if (sockfds[i] > 0 && close(sockfds[i]) == -1) {
        fprintf(stderr, "[ERROR] close failed w/ sfd = %d\n", sockfds[i]);
        perror("close");
      }
    }

    return -1;
  }

  for (size_t i = 0; i < dfc_op->n_servers; ++i) {
    sk_buf[i].sockfd = sockfds[i];
    pthread_mutex_init(&sk_buf[i].mutex, NULL);
  }

  fprintf(stderr, "[INFO] preparing files\n");

  // write pieces to accepting servers
  for (size_t i = 0; i < dfc_op->n_files; ++i) {
    srv_alloc_start = hash_djb2(dfc_op->files[i]) % dfc_op->n_servers;

    // read file
    if ((file_content = read_file(dfc_op->files[i], &len_file)) == NULL) {
      fprintf(stderr, "[ERROR] failed to read %s: %s\n", dfc_op->files[i],
              strerror(errno));
      break;
    }

    // get chunk sizes
    get_chunk_sizes(len_file, dfc_op->n_servers, chunk_sizes);
    file_pieces = split_file(file_content, chunk_sizes, dfc_op->n_servers);

    // last chunk size will always be maximum
    if ((pair = alloc_buf(chunk_sizes[dfc_op->n_servers - 1] * 2)) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    for (size_t j = 0; j < dfc_op->n_servers; ++j) {
      srv_id = (srv_alloc_start + j) % dfc_op->n_servers;
      fprintf(stderr, "[INFO] selected server %zu(%s)\n", srv_id,
              dfc_op->servers[srv_id]);

      len_pair =
          chunk_sizes[srv_id] + chunk_sizes[(srv_id + 1) % dfc_op->n_servers];

      if ((sk_buf[srv_id].data = alloc_buf(len_pair + sizeof(DFCHeader))) ==
          NULL) {
        fprintf(stderr, "[FATAL] out of memory\n");
        exit(EXIT_FAILURE);
      }

      if (sockfds[srv_id] == -1) {  // acceptable, decided beforehand
        continue;
      }

      pthread_mutex_lock(&sk_buf[srv_id].mutex);

      merge(file_pieces[srv_id], chunk_sizes[srv_id],
            file_pieces[(srv_id + 1) % dfc_op->n_servers],
            chunk_sizes[(srv_id + 1) % dfc_op->n_servers], pair);

      sk_buf[srv_id].sockfd = sockfds[srv_id];

      len_hdr = attach_hdr(
          sk_buf[srv_id].data, "put", dfc_op->files[i],
          chunk_sizes[srv_id] + chunk_sizes[(srv_id + 1) % dfc_op->n_servers]);
      memcpy(sk_buf[srv_id].data + len_hdr, pair, len_pair);

      sk_buf[srv_id].len_data = len_hdr + len_pair;

      fprintf(stderr, "[INFO] sending pieces %zu, %zu of %s to server %s\n",
              srv_id, (srv_id + 1) % dfc_op->n_servers, dfc_op->files[i],
              dfc_op->servers[srv_id]);

      pthread_mutex_unlock(&sk_buf[srv_id].mutex);

      if (pthread_create(&send_tids[j], NULL, async_dfc_send,
                         &sk_buf[srv_id])) {
        fprintf(stderr, "[%s] could not create thread %zu\n", __func__, i);
        exit(EXIT_FAILURE);
      }

      ran_threads[srv_id] = 1;
    }

    for (size_t j = 0; j < dfc_op->n_servers; ++j) {
      if (ran_threads[j] == 1) {
        pthread_join(send_tids[j], NULL);
      }
    }

    for (size_t j = 0; j < dfc_op->n_servers; ++j) {
      if (sk_buf[j].data != NULL) {
        free(sk_buf[j].data);
      }

      if (file_pieces[j] != NULL) {
        free(file_pieces[j]);
      }
    }

    if (file_pieces != NULL) {
      free(file_pieces);
    }

    if (file_content != NULL) {
      free(file_content);
    }

    if (pair != NULL) {
      free(pair);
    }
  }

  for (size_t i = 0; i < dfc_op->n_servers; ++i) {
    if (sockfds[i] > 0 && close(sockfds[i]) == -1) {
      fprintf(stderr, "[ERROR] close failed w/ sfd = %d\n", sockfds[i]);
      perror("close");
    }
  }

  return 0;
}

void print_socket_buffer(SocketBuffer *sb) {
  fputs("SocketBuffer {\n", stderr);
  fprintf(stderr, "  sockfd = %d\n===\n", sb->sockfd);
  fwrite(sb->data + sizeof(DFCHeader), sizeof(*sb->data), sb->len_data, stderr);
  fprintf(stderr, "\n===\n  len = %zu\n", sb->len_data);
  fputs("}\n", stderr);
}
