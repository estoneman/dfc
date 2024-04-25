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

#include "dfc/socket_util.h"

int connection_sockfd(const char *hostname, const char *port) {
  struct addrinfo hints, *srv_entries, *srv_entry;
  int sockfd, addrinfo_status;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

#ifdef DEBUG
  fprintf(stderr, "[%s] connecting to %s:%s\n", __func__, hostname, port);
#endif
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

    /*
     * EINPROGRESS
         The socket is nonblocking and the connection cannot be completed
       immediately. (UNIX domain sockets failed with EAGAIN instead.) It is
       possible to select(2) or poll(2) for completion by selecting the socket
       for writing. After select(2) indicates writability, use getsockopt(2) to
       read the SO_ERROR option at level SOL_SOCKET to determine whether
       connect() completed successfully (SO_ERROR is zero) or unsuccessfully
       (SO_ERROR  is one of the usual error codes listed here, explaining the
       reason for the failure).
     */
    if (connect(sockfd, srv_entry->ai_addr, srv_entry->ai_addrlen) < 0) {
      close(sockfd);
      continue;
    }

    break;  // successfully created socket and connected to remote service
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
    return -1;
  }

  return nb_sent;
}
