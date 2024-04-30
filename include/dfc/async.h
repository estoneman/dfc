#ifndef ASYNC_H_
#define ASYNC_H_

typedef struct {
  char cmd[4];
  char *filename;
  size_t offset;
} DFCHeader;
#define DFC_HDRSZ sizeof(DFCHeader)

typedef struct {
  int sockfd;
  DFCHeader *dfc_hdr;
  char *data;
  ssize_t len_data;
} SocketBuffer;

void *async_dfc_send(void *);
void *handle_get(void *);
void *handle_list(void *);
void *handle_put(void *);
void print_socket_buffer(SocketBuffer *);

void print_header(DFCHeader *);

#endif  // ASYNC_H_
