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

// static pthread_mutex_t net_io_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_rwlock_t dfc_sb_rwlock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t conf_rwlock = PTHREAD_RWLOCK_INITIALIZER;

void *async_dfc_send(void *arg) {
  DFCBuffer *dfc_buffer = (DFCBuffer *)arg;
  ssize_t bytes_sent;

  pthread_rwlock_rdlock(&dfc_sb_rwlock);

  for (size_t i = 0; i < dfc_buffer->n_sbs; ++i) {
    if ((bytes_sent = dfc_send(
             dfc_buffer->sbs[i].sockfd, dfc_buffer->sbs[i].data,
             dfc_buffer->sbs[i].len_data)) != dfc_buffer->sbs[i].len_data) {
      fprintf(stderr, "[ERROR] incomplete send: wanted=%zd, sent=%zd\n",
              dfc_buffer->sbs[i].len_data, bytes_sent);
    }

#ifdef DEBUG
    fprintf(stderr, "[%s] sent %zd bytes\n", __func__, bytes_sent);
    fflush(stderr);
#endif
  }

  pthread_rwlock_unlock(&dfc_sb_rwlock);

  return NULL;
}

void *handle_get(void *arg) {
  char *filename = *(char **)arg;

  fprintf(stderr, "[INFO] put thread got %s\n", filename);

  return NULL;
}

void *handle_put(void *arg) {
  DFCOperation *dfc_op = (DFCOperation *)arg;

  char *file_contents;
  char **chunks;
  size_t *chunk_sizes, len_file, n;
  unsigned short fname_hash, start_index;
  int sockfd;

  pthread_rwlock_rdlock(&conf_rwlock);
  fprintf(stderr, "[%s] acquired rdlock on 'conf lock'\n", __func__);

  // read input file
  if ((file_contents = read_file(dfc_op->filename, &len_file)) == NULL) {
    fprintf(stderr, "[ERROR] unable to read file %s\n", dfc_op->filename);

    return NULL;
  }

  n = dfc_op->dfc_config->n_servers;
  chunk_sizes = malloc(sizeof(size_t) * n);
  if (chk_alloc_err(chunk_sizes, "malloc", __func__, __LINE__ - 1) == -1) {
    fprintf(stderr, "[ERROR] out of memory\n");
    free(file_contents);

    exit(EXIT_FAILURE);
  }

  // split file
  get_chunk_sizes(len_file, n, chunk_sizes);
  if ((chunks = split_file(file_contents, chunk_sizes, n)) == NULL) {
    fprintf(stderr, "[ERROR] unable to chunkify %s\n", dfc_op->filename);
    free(file_contents);

    exit(EXIT_FAILURE);
  }

  // get hash of filename
  fname_hash = hash_djb2(dfc_op->filename);
  start_index = fname_hash % n;

  pthread_t send_threads[n];
  DFCBuffer dfc_buffer;
  char hostname[DFC_SERVER_NAME_MAX + 1], port[MAX_PORT_DIGITS + 1];
  ssize_t port_offset;

  if ((dfc_buffer.sbs =
           (SocketBuffer *)malloc(sizeof(SocketBuffer) * (n / 2))) == NULL) {
    fprintf(stderr, "[FATAL] out of memory\n");

    exit(EXIT_FAILURE);
  }
  dfc_buffer.n_sbs = n / 2;

  // given the last chunk size will always be greater than the rest, can
  // allocate here as opposed to in the loop
  for (size_t i = 0; i < dfc_buffer.n_sbs; ++i) {
    if ((dfc_buffer.sbs[i].data = alloc_buf(chunk_sizes[n - 1])) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");

      exit(EXIT_FAILURE);
    }
  }

  for (size_t i = 0, j; i < n; ++i) {
    j = (start_index + i) % n;

    // get hostname
    port_offset = 0;
    if ((port_offset =
             read_until(dfc_op->dfc_config->servers[j], DFC_SERVER_NAME_MAX,
                        ':', hostname, DFC_SERVER_NAME_MAX)) == -1) {
      fprintf(stderr, "[%s] error in configuration for server %zu.. exiting\n",
              __func__, j);

      exit(EXIT_FAILURE);
    }

    // get port
    if (read_until(dfc_op->dfc_config->servers[j] + port_offset,
                   MAX_PORT_DIGITS, '\0', port, MAX_PORT_DIGITS) == -1) {
      fprintf(stderr, "[%s] error in configuration for server  %zu.. exiting\n",
              __func__, j);

      exit(EXIT_FAILURE);
    }

    // connect to dfs server
    if ((sockfd = connection_sockfd(hostname, port)) == -1) {
      fprintf(stderr,
              "[%s] server %zu (%s:%s) is not responding, trying next..\n",
              __func__, j + 1, hostname, port);
      send_threads[i] = -1;

      continue;
    }

    pthread_rwlock_wrlock(&dfc_sb_rwlock);
    for (size_t k = 0; k < n / 2; ++k) {
      dfc_buffer.sbs[k].sockfd = sockfd;
      memcpy(dfc_buffer.sbs[k].data, chunks[(i + k) % n],
             chunk_sizes[(i + k) % n]);
      dfc_buffer.sbs[k].len_data = chunk_sizes[(i + k) % n];
#ifdef DEBUG
      // print_socket_buffer(&dfc_buffer.sbs[k]);
#endif
    }
    pthread_rwlock_unlock(&dfc_sb_rwlock);

#ifdef DEBUG
    fprintf(stderr,
            "[%s] sending pieces %zu(size=%zu) and %zu(size=%zu) to server %zu "
            "(%s:%s)\n",
            __func__, i, chunk_sizes[i], (i + 1) % n, chunk_sizes[(i + 1) % n],
            j + 1, hostname, port);
#endif
    if (pthread_create(&send_threads[i], NULL, async_dfc_send, &dfc_buffer) <
        0) {
      fprintf(stderr, "[ERROR] could not create thread: %s:%d\n", __func__,
              __LINE__ - 1);

      return NULL;
    }
  }

  pthread_rwlock_unlock(&conf_rwlock);
  fprintf(stderr, "[%s] released rdlock on 'conf lock'\n", __func__);

  // free resources
  for (size_t i = 0; i < n; ++i) {
    if (send_threads[i] != (long unsigned int)-1)
      pthread_join(send_threads[i], NULL);
    free(chunks[i]);
  }
  free(chunks);

  for (size_t i = 0; i < n / 2; ++i) {
    free(dfc_buffer.sbs[i].data);
  }
  free(dfc_buffer.sbs);

  free(chunk_sizes);
  free(file_contents);

  return NULL;
}

void print_socket_buffer(SocketBuffer *sb) {
  fputs("SocketBuffer {\n", stderr);
  fprintf(stderr, "  sockfd = %d\n===\n", sb->sockfd);
  fwrite(sb->data, sizeof(*sb->data), sb->len_data, stderr);
  fprintf(stderr, "\n===\n  len = %zu\n", sb->len_data);
  fputs("}\n", stderr);
}

void *read_config(void *arg) {
  DFCConfig *dfc_config = (DFCConfig *)arg;

  char line[CONF_MAXLINE + 1];
  FILE *fp;
  size_t n_cols, addr_offset, n_servers;

  pthread_rwlock_wrlock(&conf_rwlock);
  fprintf(stderr, "[%s] acquired wrlock on 'conf_rwlock'\n", __func__);
  if ((fp = fopen(DFC_CONF, "r")) == NULL) {
    fprintf(stderr, "[ERROR] unable to open %s\n", DFC_CONF);

    return NULL;
  }

  n_servers = 0;
  n_cols = 0;
  addr_offset = 0;

  while (fgets(line, CONF_MAXLINE, fp) != NULL) {
    line[strlen(line) - 1] = '\0';
    while (n_cols < 2) {
      if (line[addr_offset] == ' ') {
        n_cols++;
      }

      addr_offset++;
    }

    strncpy(dfc_config->servers[n_servers], line + addr_offset, CONF_MAXLINE);
    n_servers++;
  }

  dfc_config->n_servers = n_servers;
  pthread_rwlock_unlock(&conf_rwlock);
  fprintf(stderr, "[%s] released wrlock held by 'conf lock'\n", __func__);

  return NULL;
}
