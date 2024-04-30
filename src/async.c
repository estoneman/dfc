#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dfc/bloom_filter.h"
#include "dfc/dfc.h"
#include "dfc/dfc_util.h"
#include "dfc/socket_util.h"
#include "dfc/async.h"

static pthread_rwlock_t skb_lock = PTHREAD_RWLOCK_INITIALIZER;

void *async_dfc_send(void *arg) {
  SocketBuffer *sk_buf;
  ssize_t bytes_sent;

  pthread_rwlock_rdlock(&skb_lock);

  sk_buf = (SocketBuffer *)arg;

  // send data
  if ((bytes_sent = dfc_send(sk_buf->sockfd, sk_buf->data, sk_buf->len_data)) !=
      sk_buf->len_data) {
    fprintf(stderr, "[ERROR] incomplete send\n");
  }

  pthread_rwlock_unlock(&skb_lock);

  return NULL;
}

void *handle_get(void *arg) {
  DFCOperation *dfc_op = (DFCOperation *)arg;

  fprintf(stderr, "[INFO] put thread got %s\n", dfc_op->filename);

  return NULL;
}

void *handle_list(void *arg) {
  (void)arg;
  fputs("[INFO] list thread\n", stderr);

  return NULL;
}

void *handle_put(void *arg) {
  DFCOperation *dfc_op = (DFCOperation *)arg;

  char *file_contents, **file_pieces, *pair;
  size_t len_file, n_servers;
  int split;
  unsigned int srv_alloc_idx;

  // read input file
  if ((file_contents = read_file(dfc_op->filename, &len_file)) == NULL) {
    fprintf(stderr, "[ERROR] unable to read file %s\n", dfc_op->filename);

    return NULL;
  }

  n_servers = dfc_op->dfc_config->n_servers;
  size_t chunk_sizes[n_servers];
  // given n_servers, split file into `n_servers` pieces
  get_chunk_sizes(len_file, n_servers, chunk_sizes);
  file_pieces = split_file(file_contents, chunk_sizes, n_servers);

  // given n_servers, check if split is required (n_servers > 1)
  // cannot be zero, get_chunk_sizes checks this
  split = n_servers == 1 ? 0 : 1;

  srv_alloc_idx = hash_fnv1a(dfc_op->filename) % n_servers;
  fprintf(stderr, "start allocation index = %d\n", srv_alloc_idx);

  if ((pair = alloc_buf(chunk_sizes[n_servers - 1] * 2)) == NULL) {
    fprintf(stderr, "[FATAL] out of memory\n");
    exit(EXIT_FAILURE);
  }

  unsigned int j;
  int sockfd;
  char hostname[DFC_SERVER_NAME_MAX + 1], port[MAX_PORT_DIGITS + 1];
  ssize_t port_offset;
  pthread_t send_threads[n_servers];
  int ran_threads[n_servers];
  SocketBuffer sk_buf[n_servers];
  DFCHeader dfc_hdr;
  size_t len_pair;

  for (size_t i = 0; i < n_servers; ++i) {
    j = (srv_alloc_idx + i) % n_servers;

    // get hostname
    port_offset = 0;
    if ((port_offset =
             read_until(dfc_op->dfc_config->servers[j], DFC_SERVER_NAME_MAX,
                        ':', hostname, DFC_SERVER_NAME_MAX)) == -1) {
      fprintf(stderr, "[%s] error in configuration for server %d.. exiting\n",
              __func__, j);

      exit(EXIT_FAILURE);
    }

    // get port
    if (read_until(dfc_op->dfc_config->servers[j] + port_offset,
                   MAX_PORT_DIGITS, '\0', port, MAX_PORT_DIGITS) == -1) {
      fprintf(stderr, "[%s] error in configuration for server  %d.. exiting\n",
              __func__, j);

      exit(EXIT_FAILURE);
    }

    if (split) {
      merge(file_pieces[j], chunk_sizes[j], file_pieces[(j + 1) % n_servers],
            chunk_sizes[(j + 1) % n_servers], pair);
      len_pair = chunk_sizes[j] + chunk_sizes[(j + 1) % n_servers];
    } else {
      memcpy(pair, file_pieces[j], chunk_sizes[j]);
      len_pair = chunk_sizes[j];
    }

    // connect to server `j`
    if ((sockfd = connection_sockfd(hostname, port)) == -1) {
      fprintf(stderr, "[%s] could not connect to server %s:%s\n", __func__,
              hostname, port);

      continue;
    }

    pthread_rwlock_wrlock(&skb_lock);
    sk_buf[j].sockfd = sockfd;

    strncpy(dfc_hdr.cmd, "put", sizeof(dfc_hdr.cmd));
    dfc_hdr.filename = dfc_op->filename;
    dfc_hdr.offset = chunk_sizes[j];

    ssize_t total_len;
    size_t hdr_len;

    hdr_len =
        sizeof(dfc_hdr.cmd) + strlen(dfc_hdr.filename) + sizeof(dfc_hdr.offset);

    // allocate space for header and data
    if ((sk_buf[j].data = alloc_buf(len_pair + hdr_len)) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    // copy header
    memcpy(sk_buf[j].data, dfc_hdr.cmd, sizeof(dfc_hdr.cmd));
    memcpy(sk_buf[j].data + sizeof(dfc_hdr.cmd), dfc_hdr.filename,
           strlen(dfc_hdr.filename));
    memcpy(sk_buf[j].data + sizeof(dfc_hdr.cmd) + strlen(dfc_hdr.filename),
           &dfc_hdr.offset, sizeof(dfc_hdr.offset));

    // copy data
    memcpy(sk_buf[j].data + hdr_len, pair, len_pair);
    total_len = hdr_len + len_pair;

    sk_buf[j].len_data = total_len;
    pthread_rwlock_unlock(&skb_lock);

    // send `dfc_hdr` + `pair` to server `j`
#ifdef DEBUG
    fprintf(stderr, "[%s] sending pieces %d and %zu to %s:%s\n", __func__, j,
            (j + 1) % n_servers, hostname, port);
#endif
    if (pthread_create(&send_threads[j], NULL, async_dfc_send, &sk_buf[j]) ==
        -1) {
      fprintf(stderr, "[%s] could not create thread %zu\n", __func__, i);
      exit(EXIT_FAILURE);
    }

    ran_threads[j] = 1;
  }

  for (size_t i = 0; i < n_servers; ++i) {
    if (ran_threads[i] == 1) {
      pthread_join(send_threads[i], NULL);

      if (close(sk_buf[i].sockfd) == -1) {
        perror("close");
      }
    }
  }

  free(pair);

  return NULL;
}

void print_header(DFCHeader *dfc_hdr) {
  fputs("DFCHeader {\n", stderr);
  fprintf(stderr, "  cmd: %s\n", dfc_hdr->cmd);
  fprintf(stderr, "  filename: %s\n", dfc_hdr->filename);
  fprintf(stderr, "  offset: %zu\n", dfc_hdr->offset);
  fputs("}\n", stderr);
}

void print_socket_buffer(SocketBuffer *sb) {
  fputs("SocketBuffer {\n", stderr);
  fprintf(stderr, "  sockfd = %d\n===\n", sb->sockfd);
  fwrite(sb->data + sizeof(DFCHeader), sizeof(*sb->data), sb->len_data, stderr);
  fprintf(stderr, "\n===\n  len = %zu\n", sb->len_data);
  fputs("}\n", stderr);
}
