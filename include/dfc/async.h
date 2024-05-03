#ifndef ASYNC_H_
#define ASYNC_H_

void *async_dfc_send(void *);
void *handle_get(void *);
void *handle_list(void *);
int handle_put(char *, int *, size_t);
void print_socket_buffer(SocketBuffer *);

#endif  // ASYNC_H_
