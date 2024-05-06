#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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

#define MAX_GET (5 * 1024)
#define MAX_LIST 64

static pthread_mutex_t sk_mutex = PTHREAD_MUTEX_INITIALIZER;

void *async_dfc_recv(void *arg) {
  SocketBuffer *sk_buf;

  sk_buf = (SocketBuffer *)arg;

  // recv data
  pthread_mutex_lock(&sk_mutex);
  sk_buf->data = dfc_recv(sk_buf->sockfd, &(sk_buf->len_data));
  fprintf(stderr, "[%s] received %zd bytes over sfd=%d\n", __func__, sk_buf->len_data, sk_buf->sockfd);
  pthread_mutex_unlock(&sk_mutex);

  return NULL;
}

void *async_dfc_send(void *arg) {
  SocketBuffer *sk_buf;
  ssize_t bytes_sent;

  sk_buf = (SocketBuffer *)arg;

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

  return NULL;
}

void *get_handle(void *arg) {
  GetOperation *get_op = (GetOperation *)arg;

  unsigned int srv_alloc_start;
  size_t srv_id;

  DFCHeader dfc_hdr;
  SocketBuffer hdr_sk_buf[get_op->n_servers];
  SocketBuffer rcv_sk_buf[get_op->n_servers];
  int ran[get_op->n_servers];

  srv_alloc_start = hash_djb2(get_op->fname) % get_op->n_servers;

  for (size_t i = 0; i < get_op->n_servers; ++i) {
    srv_id = (srv_alloc_start + i) % get_op->n_servers;

    if (get_op->sockfds[srv_id] <= 0) {  // acceptable, decided beforehand
      continue;
    }

    if ((hdr_sk_buf[srv_id].data = alloc_buf(sizeof(DFCHeader))) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    // form request
    memset(&dfc_hdr, 0, sizeof(DFCHeader));
    strncpy(dfc_hdr.cmd, "get", sizeof(dfc_hdr.cmd));
    strncpy(dfc_hdr.fname, get_op->fname, sizeof(dfc_hdr.fname));
    memcpy(hdr_sk_buf[srv_id].data, &dfc_hdr, sizeof(DFCHeader));

    // send request
    hdr_sk_buf[srv_id].len_data = sizeof(dfc_hdr);
    hdr_sk_buf[srv_id].sockfd = get_op->sockfds[srv_id];
    if (write(hdr_sk_buf[srv_id].sockfd, hdr_sk_buf[srv_id].data, hdr_sk_buf[srv_id].len_data) == -1) {
      perror("write");
      exit(EXIT_FAILURE);
    }

    ran[srv_id] = 1;
  }

  for (size_t i = 0; i < get_op->n_servers; ++i) {
    srv_id = (srv_alloc_start + i) % get_op->n_servers;
    // recv response
    rcv_sk_buf[srv_id].sockfd = get_op->sockfds[srv_id];
    if ((rcv_sk_buf[srv_id].data = dfc_recv(rcv_sk_buf[srv_id].sockfd,
                                            &(rcv_sk_buf[srv_id].len_data)))
         == NULL) {
      exit(EXIT_FAILURE);
    }
  }

  // reassemble
  size_t chunk_offset, next;
  size_t chunk_sizes[get_op->n_servers];
  char *chunks[get_op->n_servers];
  for (size_t i = 0; i < get_op->n_servers; ++i) {
    srv_id = (srv_alloc_start + i) % get_op->n_servers;
    next = (srv_id + 1) % get_op->n_servers;

    if (ran[srv_id]) {
      memcpy(&chunk_offset, rcv_sk_buf[srv_id].data, sizeof(size_t));

      fprintf(stderr, "current pieces = %zu and %zu\n", srv_id, next);
      fprintf(stderr, "chunk offset = %zu\n", chunk_offset);

      chunks[srv_id] = rcv_sk_buf[srv_id].data + sizeof(size_t);
      chunk_sizes[srv_id] = chunk_offset;

      chunks[next] = rcv_sk_buf[srv_id].data + sizeof(size_t) + chunk_offset;
      chunk_sizes[next] = rcv_sk_buf[srv_id].len_data - sizeof(size_t) - chunk_offset;
    }
  }

  int fd;
  ssize_t bytes_written;

  if ((fd = open(get_op->fname, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, S_IWUSR | S_IRUSR)) == -1) {
    perror("open");
    exit(EXIT_FAILURE);
  }

  for (size_t i = 0; i < get_op->n_servers; ++i) {
    if ((bytes_written = write(fd, chunks[i], chunk_sizes[i])) == -1) {
      perror("write");
      exit(EXIT_FAILURE);
    }

    fprintf(stderr, "[%s] wrote %zd bytes to %s\n", __func__, bytes_written, get_op->fname);
  }

  if (close(fd) == -1) {
    perror("close");
    exit(EXIT_FAILURE);
  }

  for (size_t i = 0; i < get_op->n_servers; ++i) {
    if (hdr_sk_buf[i].data != NULL)
      free(hdr_sk_buf[i].data);
    
    if (rcv_sk_buf[i].data != NULL)
      free(rcv_sk_buf[i].data);
  }

  return NULL;
}

void *list_handle(void *arg) {
  int list_fd = *(int *)arg;

  DFCHeader dfc_hdr;
  SocketBuffer hdr_sk_buf;
  SocketBuffer rcv_sk_buf;

  pthread_t rcv_tid;
  pthread_t snd_tid;

  if ((hdr_sk_buf.data = alloc_buf(sizeof(DFCHeader))) == NULL) {
    fprintf(stderr, "[FATAL] out of memory\n");
    exit(EXIT_FAILURE);
  }

  // form request
  memset(&dfc_hdr, 0, sizeof(DFCHeader));
  strncpy(dfc_hdr.cmd, "list", sizeof(dfc_hdr.cmd));
  memcpy(hdr_sk_buf.data, &dfc_hdr, sizeof(DFCHeader));

  // send request
  hdr_sk_buf.len_data = sizeof(dfc_hdr);
  hdr_sk_buf.sockfd = list_fd;
  if (pthread_create(&snd_tid, NULL, async_dfc_send, &hdr_sk_buf) < 0) {
    fprintf(stderr, "[%s] could not create thread\n", __func__);
    exit(EXIT_FAILURE);
  }

  rcv_sk_buf.sockfd = list_fd;
  if (pthread_create(&rcv_tid, NULL, async_dfc_recv, &rcv_sk_buf) < 0) {
    fprintf(stderr, "[%s] failed to create thread\n", __func__);
    exit(EXIT_FAILURE);
  }

  pthread_join(snd_tid, NULL);
  pthread_join(rcv_tid, NULL);

  size_t len_entry;
  for (size_t i = 0; i < (size_t)rcv_sk_buf.len_data; ) {
    len_entry = strlen(rcv_sk_buf.data + i) + 1;
    printf("%s\n", rcv_sk_buf.data + i);
    i += len_entry;
  }

  return NULL;
}

void *put_handle(void *arg) {
  PutOperation *put_op = (PutOperation *)arg;
  SocketBuffer data_sk_buf[put_op->n_servers];
  SocketBuffer hdr_sk_buf[put_op->n_servers];
  DFCHeader dfc_hdr;
  pthread_t send_tids[put_op->n_servers];
  int ran_threads[put_op->n_servers];
  char **file_pieces, *file_content, *pair;
  size_t len_pair, srv_id, chunk_sizes[put_op->n_servers];
  ssize_t len_file;
  unsigned srv_alloc_start;

  srv_alloc_start = hash_djb2(put_op->fname) % put_op->n_servers;

  if ((file_content = read_file(put_op->fname, &len_file)) == NULL) {
    fprintf(stderr, "[ERROR] failed to read %s: %s\n", put_op->fname, strerror(errno));
    return NULL;
  }

  // get chunk sizes
  get_chunk_sizes(len_file, put_op->n_servers, chunk_sizes);
  file_pieces = split_file(file_content, chunk_sizes, put_op->n_servers);

  // last chunk size will always be maximum
  if ((pair = alloc_buf(chunk_sizes[put_op->n_servers - 1] * 2)) == NULL) {
    fprintf(stderr, "[FATAL] out of memory\n");
    exit(EXIT_FAILURE);
  }

  for (size_t i = 0; i < put_op->n_servers; ++i) {
    srv_id = (srv_alloc_start + i) % put_op->n_servers;

    if (put_op->sockfds[srv_id] <= 0) {  // acceptable, decided beforehand
      continue;
    }

    fprintf(stderr, "[INFO] selected server %zu\n", srv_id);

    len_pair =
        chunk_sizes[srv_id] + chunk_sizes[(srv_id + 1) % put_op->n_servers];

    if ((data_sk_buf[srv_id].data = alloc_buf(len_pair)) ==
        NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    if ((hdr_sk_buf[srv_id].data = alloc_buf(sizeof(DFCHeader))) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    merge(file_pieces[srv_id], chunk_sizes[srv_id],
          file_pieces[(srv_id + 1) % put_op->n_servers],
          chunk_sizes[(srv_id + 1) % put_op->n_servers], pair);

    memset(&dfc_hdr, 0, sizeof(DFCHeader));
    strncpy(dfc_hdr.cmd, "put", sizeof(dfc_hdr.cmd));
    strncpy(dfc_hdr.fname, put_op->fname, sizeof(dfc_hdr.fname));

    // where next piece starts
    dfc_hdr.chunk_offset = chunk_sizes[srv_id];
    // where next file starts
    dfc_hdr.file_offset = chunk_sizes[srv_id] + chunk_sizes[(srv_id + 1) % put_op->n_servers];
    // attach_hdr(hdr_sk_buf[srv_id].data, &dfc_hdr);
    memcpy(hdr_sk_buf[srv_id].data, &dfc_hdr, sizeof(DFCHeader));

    ssize_t bytes_written;
    hdr_sk_buf[srv_id].sockfd = put_op->sockfds[srv_id];
    if ((bytes_written = write(hdr_sk_buf[srv_id].sockfd, hdr_sk_buf[srv_id].data, sizeof(dfc_hdr))) == -1) {
      perror("send");
      exit(EXIT_FAILURE);
    }
    fprintf(stderr, "wrote %zd bytes of header to sfd=%d\n", bytes_written, hdr_sk_buf[srv_id].sockfd);

    data_sk_buf[srv_id].sockfd = put_op->sockfds[srv_id];
    data_sk_buf[srv_id].len_data = len_pair;
    memcpy(data_sk_buf[srv_id].data, pair, len_pair);

    fprintf(stderr, "[INFO] sending pieces %zu, %zu of %s\n",
            srv_id, (srv_id + 1) % put_op->n_servers, put_op->fname);

    if (pthread_create(&send_tids[srv_id], NULL, async_dfc_send,
                       &data_sk_buf[srv_id])) {
      fprintf(stderr, "[%s] could not create thread %zu\n", __func__, i);
      exit(EXIT_FAILURE);
    }

    ran_threads[srv_id] = 1;
  }

  for (size_t i = 0; i < put_op->n_servers; ++i) {
    if (ran_threads[i] == 1) {
      pthread_join(send_tids[i], NULL);
    }
  }

  for (size_t i = 0; i < put_op->n_servers; ++i) {
    if (hdr_sk_buf[i].data != NULL)
      free(hdr_sk_buf[i].data);

    if (data_sk_buf[i].data != NULL)
      free(data_sk_buf[i].data);

    if (file_pieces[i] != NULL)
      free(file_pieces[i]);
  }

  free(file_pieces);
  free(file_content);
  free(pair);

  return NULL;
}

void print_socket_buffer(SocketBuffer *sb) {
  fputs("SocketBuffer {\n", stderr);
  fprintf(stderr, "  sockfd = %d\n===\n", sb->sockfd);
  fwrite(sb->data + sizeof(DFCHeader), sizeof(*sb->data), sb->len_data, stderr);
  fprintf(stderr, "\n===\n  len = %zu\n", sb->len_data);
  fputs("}\n", stderr);
}
