#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "dfc/dfc_util.h"

char *alloc_buf(size_t size) {
  char *buf;

  buf = (char *)malloc(size);
  if (chk_alloc_err(buf, "malloc", __func__, __LINE__ - 1) == -1) {
    return NULL;
  }

  return buf;
}

int chk_alloc_err(void *mem, const char *allocator, const char *func,
                  int line) {
  if (mem == NULL) {
    fprintf(stderr, "%s failed @%s:%d\n", allocator, func, line);
    return -1;
  }

  return 0;
}

void free_buf(char *buf) {
  if (buf != NULL) {
    free(buf);
  }
}

void get_chunk_sizes(size_t file_size, size_t n_chunks, size_t *out) {
  size_t chunk;
  size_t sum;

  // === BEGIN FAILURE MODES ===
  if (out == NULL) {
    fprintf(stderr, "[ERROR] 'out' has not been allocated.. exiting\n");

    exit(EXIT_FAILURE);
  }

  if (file_size == 0 || n_chunks == 0 || n_chunks > file_size) {
    fprintf(stderr, "[ERROR] cannot partition file.. exiting\n");
    fprintf(stderr, "[INFO] fsize=%zu, n_chunks=%zu\n", file_size, n_chunks);

    exit(EXIT_FAILURE);
  }
  // === END FAILURE MODES ===

  if (n_chunks == 1) {
    out[0] = file_size;

    return;
  }

  if (file_size % n_chunks == 0) {
    for (size_t i = 0; i < n_chunks; ++i) {
      out[i] = file_size / n_chunks;
    }

    return;
  }

  sum = 0;
  for (chunk = 0; chunk < n_chunks - 1; ++chunk) {
    out[chunk] = file_size / n_chunks;
    sum += out[chunk];
  }

  out[chunk] = file_size - sum;
}

void merge(char *p1, size_t len_p1, char *p2, size_t len_p2, char *out) {
// #ifdef DEBUG
//   fputs("=== MERGE BEGIN ===\n", stderr);
//   fwrite(p1, sizeof(*p1), len_p1, stderr);
//   fputs("\n----------\n", stderr);
//   fwrite(p2, sizeof(*p2), len_p2, stderr);
//   fputs("=== MERGE END ===\n", stderr);
//   fflush(stderr);
// #endif
  memcpy(out, p1, len_p1);
  memcpy(out + len_p1, p2, len_p2);
}

DFCConfig *read_config() {
  char line[CONF_MAXLINE + 1];
  FILE *fp;
  size_t n_cols, addr_offset, n_servers;
  DFCConfig *dfc_config;

  if ((dfc_config = (DFCConfig *)malloc(sizeof(DFCConfig))) == NULL) {
    fprintf(stderr, "[FATAL] out of memory\n");
    exit(EXIT_FAILURE);
  }

  if ((dfc_config->servers = (char **)malloc(sizeof(char *) * MAX_SERVERS)) == NULL) {
    fprintf(stderr, "[ERORR] out of memory\n");

    exit(EXIT_FAILURE);
  }

  for (size_t i = 0; i < MAX_SERVERS; ++i) {
    if ((dfc_config->servers[i] = (char *)alloc_buf(CONF_MAXLINE)) == NULL) {
      fprintf(stderr, "[ERROR] out of memory\n");

      exit(EXIT_FAILURE);
    }
  }

  if ((fp = fopen(DFC_CONF, "r")) == NULL) {
    fprintf(stderr, "[ERROR] unable to open %s\n", DFC_CONF);

    return NULL;
  }

  n_servers = 0;
  n_cols = 0;
  addr_offset = 0;

  while (fgets(line, CONF_MAXLINE, fp) != NULL) {
    line[strlen(line) - 1] = '\0';
    while (n_cols < 2) {
      if (line[addr_offset] == ' ') {
        n_cols++;
      }

      addr_offset++;
    }

    strncpy(dfc_config->servers[n_servers], line + addr_offset, CONF_MAXLINE);
    n_servers++;
  }

  fclose(fp);

  dfc_config->n_servers = n_servers;

  return dfc_config;
}

char *read_file(const char *fpath, size_t *nb_read) {
  char *out_buf;
  FILE *fp;
  struct stat st;

  if ((fp = fopen(fpath, "rb")) == NULL) {
    // server error
    return NULL;
  }

  if (stat(fpath, &st) < 0) {
    // server error
    fclose(fp);

    return NULL;
  }

  out_buf = alloc_buf(st.st_size);
  chk_alloc_err(out_buf, "malloc", __func__, __LINE__ - 1);

  if ((*nb_read = fread(out_buf, 1, st.st_size, fp)) < (size_t)st.st_size) {
    fclose(fp);
    free(out_buf);

    return NULL;
  }

  fclose(fp);

  return out_buf;
}

ssize_t read_until(char *haystack, size_t len_haystack, char end, char *sink,
                   size_t len_sink) {
  // move past input
  while (isspace(*haystack)) {
    haystack += 1;
  }

  // up to the length of the input buffer, read as many characters that are
  // allowed in `sink`
  size_t i;
  for (i = 0; i < len_haystack && i < len_sink && haystack[i] != end; ++i) {
    sink[i] = haystack[i];
  }

  // if `end` not found
  if (haystack[i] != end) {
    return -1;
  }

  sink[i] = '\0';

  // move pointer to next character
  return (ssize_t)i + 1;
}

char **split_file(char *file_contents, size_t *chunk_sizes, size_t n_chunks) {
  char **chunks;
  size_t offset;

  chunks = (char **)alloc_buf(sizeof(char *) * n_chunks);
  if (chk_alloc_err(chunks, "malloc", __func__, __LINE__ - 1) == -1) {
    fprintf(stderr, "[ERROR] out of memory\n");

    return NULL;
  }

  for (size_t i = 0; i < n_chunks; ++i) {
    chunks[i] = NULL;
    if ((chunks[i] = alloc_buf(chunk_sizes[i])) == NULL) {
      fprintf(stderr, "[ERROR] out of memory\n");

      return NULL;
    }
  }

  offset = 0;
  for (size_t i = 0; i < n_chunks; ++i) {
    memcpy(chunks[i], file_contents + offset, chunk_sizes[i]);

    offset += chunk_sizes[i];
  }

  return chunks;
}
