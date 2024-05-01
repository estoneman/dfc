#ifndef TYPES_H_
#define TYPES_H_

#include <limits.h>

#define CONF_MAXLINE 1024
#define DFC_CONF "./dfc.conf"
#define MAX_SERVERS 10
#define MAX_FNAME SZ_ARG_MAX
#define SZ_ARG_MAX 1024
#define SZ_CMD_MAX 8

typedef struct {
  char cmd[SZ_CMD_MAX];
  unsigned short hash;
} DFCCommand;

typedef struct {
  char **files;
  size_t n_files;
  char **servers;
  size_t n_servers;
} DFCOperation;

#define N_CMD_SUPP sizeof(dfc_cmds) / sizeof(dfc_cmds[0])

typedef struct {
  char cmd[SZ_CMD_MAX];
  char fname[PATH_MAX];
  size_t offset;
} DFCHeader;
#define DFC_HDRSZ sizeof(DFCHeader)

typedef struct {
  int sockfd;
  char *data;
  ssize_t len_data;
  pthread_mutex_t mutex;
} SocketBuffer;

#endif  // TYPES_H_
