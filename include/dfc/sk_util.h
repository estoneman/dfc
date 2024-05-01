#ifndef SK_UTIL_H_
#define SK_UTIL_H_

#define CONNECTTIMEO_USEC 0
#define CONNECTTIMEO_SEC 1

int adjacent_failure(int *, size_t);
int connection_sockfd(const char *, const char *);
void set_timeout(int, long, long);
ssize_t dfc_send(int, char *, size_t);

#endif  // SK_UTIL_H_
