#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "dfc/types.h"
#include "dfc/dfc_util.h"
#include "dfc/sk_util.h"

int adjacent_failure(int *sockfds, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    if (sockfds[i] == -1 && sockfds[(i + 1) % len] == -1) {
      return 1;
    }
  }

  return 0;
}

int connection_sockfd(const char *hostname, const char *port) {
  struct addrinfo hints, *srv_entries, *srv_entry;
  int sockfd, addrinfo_status;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((addrinfo_status = getaddrinfo(hostname, port, &hints, &srv_entries)) <
      0) {
    fprintf(stderr, "[ERROR] getaddrinfo: %s\n", gai_strerror(addrinfo_status));

    return -1;
  }

  // loop through results of call to getaddrinfo
  for (srv_entry = srv_entries; srv_entry != NULL;
       srv_entry = srv_entry->ai_next) {
    // create socket through which server communication will be facililated
    if ((sockfd = socket(srv_entry->ai_family, srv_entry->ai_socktype,
                         srv_entry->ai_protocol)) < 0) {
      perror("socket");
      continue;
    }

    fcntl(sockfd, F_SETFL, O_NONBLOCK);

    if (connect(sockfd, srv_entry->ai_addr, srv_entry->ai_addrlen) == -1) {
      if (errno != EINPROGRESS) {
        perror("connect");
        close(sockfd);
        continue;
      }
    }

    break;
  }

  if (srv_entry == NULL) {
    freeaddrinfo(srv_entries);
    return -1;
  }

  freeaddrinfo(srv_entries);

  return sockfd;
}

char *dfc_recv(int sockfd, ssize_t *nb_recv) {
  char *recv_buf;
  size_t total_nb_recv, num_reallocs, bytes_alloced, realloc_sz;

  if ((recv_buf = alloc_buf(RCVCHUNK)) == NULL) {
    fprintf(stderr, "failed to allocate receive buffer (%s:%d)", __func__,
            __LINE__ - 1);
    return NULL;
  }

  bytes_alloced = RCVCHUNK;

  total_nb_recv = realloc_sz = num_reallocs = 0;
  while ((*nb_recv = recv(sockfd, recv_buf + total_nb_recv, RCVCHUNK, 0)) >
         0) {
    total_nb_recv += *nb_recv;

    if (total_nb_recv + RCVCHUNK >= bytes_alloced) {
      realloc_sz = bytes_alloced * 2;
      if ((recv_buf = realloc_buf(recv_buf, realloc_sz)) == NULL) {
        fprintf(stderr, "[FATAL] out of memory: attempted realloc size = %zu\n",
                realloc_sz);
        free(recv_buf);  // free old buffer

        exit(EXIT_FAILURE);
      }
    }

    bytes_alloced = realloc_sz;
    num_reallocs++;
  }

  *nb_recv = total_nb_recv;

  if (total_nb_recv == 0) {  // timeout
    perror("recv");
    free(recv_buf);

    return NULL;
  }

  return recv_buf;
}

ssize_t dfc_send(int sockfd, char *send_buf, size_t len_send_buf) {
  ssize_t nb_sent;

  if ((nb_sent = send(sockfd, send_buf, len_send_buf, 0)) < 0) {
    perror("send");
  }

  return nb_sent;
}

void fill_sk_set(DFCOperation *dfc_op, int *sockfds) {
  char hostname[DFC_SERVER_NAME_MAX + 1], port[MAX_PORT_DIGITS + 1];
  ssize_t port_offset;
  fd_set writefds;
  struct timeval timeout;
  int sel_res;

  for (size_t i = 0; i < dfc_op->n_servers; ++i) {
    // extract hostname
    port_offset = 0;
    if ((port_offset = read_until(dfc_op->servers[i], DFC_SERVER_NAME_MAX, ':',
                                  hostname, DFC_SERVER_NAME_MAX)) == -1) {
      fprintf(stderr, "[%s] error in configuration for server %zu.. exiting\n",
              __func__, i);

      exit(EXIT_FAILURE);
    }

    // extract port
    if (read_until(dfc_op->servers[i] + port_offset, MAX_PORT_DIGITS, '\0',
                   port, MAX_PORT_DIGITS) == -1) {
      fprintf(stderr, "[%s] error in configuration for server %zu.. exiting\n",
              __func__, i);

      exit(EXIT_FAILURE);
    }

    if ((sockfds[i] = connection_sockfd(hostname, port)) == -1) {
      perror("connect");
      fprintf(stderr,
              "[ERROR] connection attempt to server %zu(%s:%s) failed\n", i,
              hostname, port);
      continue;
    }
  }

  FD_ZERO(&writefds);
  for (size_t i = 0; i < dfc_op->n_servers; ++i) {
    FD_SET(sockfds[i], &writefds);
  }
  timeout.tv_sec = CONNECTTIMEO_SEC;
  timeout.tv_usec = CONNECTTIMEO_USEC;

  for (size_t i = 0; i < dfc_op->n_servers; ++i) {
    if ((sel_res = select(sockfds[dfc_op->n_servers - 1] + 1, NULL, &writefds,
                          NULL, &timeout)) > 1) {
      int so_error;
      socklen_t len = sizeof(so_error);

      getsockopt(sockfds[i], SOL_SOCKET, SO_ERROR, &so_error, &len);

      if (so_error == 0) {
        // clear non-blocking flag, if set, leads to incomplete sends/writes
        // when file is too large for socket buffer
        int flags = fcntl(sockfds[i], F_GETFL);
        flags &= ~O_NONBLOCK;
        fcntl(sockfds[i], F_SETFL, flags);
#ifdef DEBUG
        fprintf(stderr, "[INFO] %s(sfd=%d) is open\n", dfc_op->servers[i],
                sockfds[i]);
        fflush(stderr);
#endif
      } else if (so_error == ECONNREFUSED) {
#ifdef DEBUG
        fprintf(stderr, "[ERROR] (%s) %s\n", dfc_op->servers[i],
                strerror(so_error));
        fflush(stderr);
#endif
        sockfds[i] = -1;
      } else {
        perror("connect");

        exit(EXIT_FAILURE);
      }
    }
  }
}

void set_timeout(int sockfd, long tv_sec, long tv_usec) {
  struct timeval rcvtimeo;

  rcvtimeo.tv_sec = tv_sec;
  rcvtimeo.tv_usec = tv_usec;
  if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &rcvtimeo, sizeof(rcvtimeo)) <
      0) {
    fprintf(stderr, "[%s] failed to setsockopt (sfd=%d): %s\n", __func__,
            sockfd, strerror(errno));
    exit(EXIT_FAILURE);
  }
}
