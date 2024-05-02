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

ssize_t dfc_send(int sockfd, char *send_buf, size_t len_send_buf) {
  ssize_t nb_sent;

  if ((nb_sent = send(sockfd, send_buf, len_send_buf, 0)) < 0) {
    perror("send");
  }

  return nb_sent;
}

void set_timeout(int sockfd, long tv_sec, long tv_usec) {
  struct timeval rcvtimeo;

  rcvtimeo.tv_sec = tv_sec;
  rcvtimeo.tv_usec = tv_usec;
  if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &rcvtimeo, sizeof(rcvtimeo)) <
      0) {
    perror("setsockopt");
    close(sockfd);
    exit(EXIT_FAILURE);
  }
}
