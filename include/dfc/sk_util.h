#ifndef SK_UTIL_H_
#define SK_UTIL_H_

#define CONNECTTIMEO_USEC 0
#define CONNECTTIMEO_SEC 1
#define RCVTIMEO_SEC 5
#define RCVTIMEO_USEC 0
#define RCVCHUNK 1024

int adjacent_failure(int *, size_t);
int connection_sockfd(const char *, const char *);
char *dfc_recv(int, ssize_t *);
ssize_t dfc_send(int, char *, size_t);
void fill_sk_set(DFCOperation *, int *);
void set_timeout(int, long, long);

#endif  // SK_UTIL_H_
