#ifndef ASYNC_H_
#define ASYNC_H_

typedef struct {
  int sockfd;
  char *data;
  ssize_t len_data;
} SocketBuffer;

typedef struct {
  SocketBuffer *sbs;
  size_t n_sbs;
} DFCBuffer;

void *async_dfc_send(void *);
void *handle_get(void *);
void *handle_put(void *);
void print_socket_buffer(SocketBuffer *);
void *read_config(void *);

#endif  // ASYNC_H_
