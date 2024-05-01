#ifndef DFC_H_
#define DFC_H_

#include "dfc/types.h"

int run_handler(int, char **);
void usage(const char *);

// debug functions
void print_cmd(DFCCommand dfc_cmd);
void print_cmds(void);
void print_op(DFCOperation *);

#endif  // DFC_H_
