#ifndef TYPES_H_
#define TYPES_H_

#include <limits.h>

#define CONF_MAXLINE 1024
#define DFC_CONF "./dfc.conf"
#define MAX_SERVERS 10
#define MAX_FNAME SZ_ARG_MAX
#define SZ_ARG_MAX 1024
#define SZ_CMD_MAX 8

// put: offset => where next file starts
// get: offset => where next piece starts
// list: offset => unused
typedef struct {
  char cmd[SZ_CMD_MAX + 1];
  char fname[PATH_MAX + 1];
  size_t chunk_offset;  // offset at which next chunk starts
  size_t file_offset;  // offset at which next file starts
} DFCHeader;

typedef struct {
  char cmd[SZ_CMD_MAX];
  unsigned short hash;
} DFCCommand;

typedef struct {
  char fname[PATH_MAX + 1];
  char **servers;
  size_t n_servers;
} DFCOperation;

typedef struct {
  char fname[PATH_MAX + 1];
  int *sockfds;
  size_t n_servers;
} GetOperation;

typedef struct {
  char fname[PATH_MAX + 1];
  int *sockfds;
  size_t n_servers;
} PutOperation;

#define N_CMD_SUPP sizeof(dfc_cmds) / sizeof(dfc_cmds[0])

typedef struct {
  int sockfd;
  char *data;
  ssize_t len_data;
} SocketBuffer;

#endif  // TYPES_H_
