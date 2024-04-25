#ifndef SOCKET_UTIL_H_
#define SOCKET_UTIL_H_

int connection_sockfd(const char *, const char *);
ssize_t dfc_send(int, char *, size_t);

#endif  // SOCKET_UTIL_H_
