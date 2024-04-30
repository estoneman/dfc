#ifndef DFC_H_
#define DFC_H_

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
  char **servers;
  size_t n_servers;
} DFCConfig;

typedef struct {
  char filename[PATH_MAX + 1];
  DFCConfig *dfc_config;
} DFCOperation;

#define N_CMD_SUPP sizeof(dfc_cmds) / sizeof(dfc_cmds[0])

int run_handler(char **, unsigned int, size_t);
void usage(const char *);

// debug functions
void print_cmd(DFCCommand dfc_cmd);
void print_cmds(void);
void print_config(DFCConfig *);
void print_op(DFCOperation *);

#endif  // DFC_H_
