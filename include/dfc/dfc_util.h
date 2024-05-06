#ifndef DFC_UTIL_H
#define DFC_UTIL_H

#include "dfc/dfc.h"

#define UINT_SZ sizeof(unsigned int)
#define DFC_SERVER_NAME_MAX 128
#define MAX_PORT_DIGITS 5

char *alloc_buf(size_t);
size_t attach_hdr(char *, DFCHeader *);
int chk_alloc_err(void *, const char *, const char *, int);
void free_buf(char *);
void get_chunk_sizes(size_t, size_t, size_t *);
void merge(char *, size_t, char *, size_t, char *);
DFCOperation *read_config(void);
char *read_file(const char *, ssize_t *);
ssize_t read_until(char *, size_t, char, char *, size_t);
char *realloc_buf(char *, size_t);
char **split_file(char *, size_t *, size_t);

void print_header(DFCHeader *);

#endif  // DFC_UTIL_H
