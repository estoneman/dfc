#ifndef ASYNC_H_
#define ASYNC_H_

void *async_dfc_send(void *);
void *get_handle(void *);
void *list_handle(void *);
void *put_handle(void *);
int handle_put(char *, int *, size_t);
void print_socket_buffer(SocketBuffer *);

#endif  // ASYNC_H_
