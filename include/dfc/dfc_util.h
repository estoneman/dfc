#ifndef DFC_UTIL_H
#define DFC_UTIL_H

#define UINT_SZ sizeof(unsigned int)

char *alloc_buf(size_t);
int chk_alloc_err(void *, const char *, const char *, int);
void free_buf(char *);
void get_chunk_sizes(size_t, size_t, size_t *);
char *read_file(const char *, size_t *);
char **split_file(char *, size_t *, size_t);

#endif  // DFC_UTIL_H
