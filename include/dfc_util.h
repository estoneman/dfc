#ifndef DFC_UTIL_H_
#define DFC_UTIL_H_

#define MAX_CHUNKS 10

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void get_chunks(size_t, size_t, size_t *);

#endif  // DFC_UTIL_H_
