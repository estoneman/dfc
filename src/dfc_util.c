#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "dfc/dfc_util.h"

char *alloc_buf(size_t size) {
  char *buf;

  buf = (char *)malloc(size);
  if (chk_alloc_err(buf, "malloc", __func__, __LINE__ - 1) == -1) {
    return NULL;
  }

  return buf;
}

size_t attach_hdr(char *buf, DFCHeader *dfc_hdr) {
  size_t len_hdr;

  len_hdr = 0;

  memcpy(buf, dfc_hdr->cmd, sizeof(dfc_hdr->cmd));
  len_hdr += sizeof(dfc_hdr->cmd);

  memcpy(buf + len_hdr, dfc_hdr->fname, sizeof(dfc_hdr->fname));
  len_hdr += sizeof(dfc_hdr->fname);

  memcpy(buf + len_hdr, &dfc_hdr->chunk_offset, sizeof(size_t));
  len_hdr += sizeof(size_t);

  memcpy(buf + len_hdr, &dfc_hdr->file_offset, sizeof(size_t));
  len_hdr += sizeof(size_t);

  return len_hdr;
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
  memcpy(out, p1, len_p1);
  memcpy(out + len_p1, p2, len_p2);
}

DFCOperation *read_config() {
  char line[CONF_MAXLINE + 1];
  FILE *fp;
  size_t n_cols, addr_offset, n_servers;
  DFCOperation *dfc_op;

  if ((dfc_op = (DFCOperation *)malloc(sizeof(DFCOperation))) == NULL) {
    fprintf(stderr, "[FATAL] out of memory\n");
    exit(EXIT_FAILURE);
  }

  if ((dfc_op->servers = (char **)malloc(sizeof(char *) * MAX_SERVERS)) ==
      NULL) {
    fprintf(stderr, "[ERORR] out of memory\n");

    exit(EXIT_FAILURE);
  }

  for (size_t i = 0; i < MAX_SERVERS; ++i) {
    if ((dfc_op->servers[i] = (char *)alloc_buf(CONF_MAXLINE)) == NULL) {
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

    strncpy(dfc_op->servers[n_servers], line + addr_offset, CONF_MAXLINE);
    n_servers++;
  }

  if (n_servers == 0) {
    for (size_t i = 0; i < MAX_SERVERS; ++i) {
      free(dfc_op->servers[i]);
    }

    free(dfc_op->servers);
    fclose(fp);

    return NULL;
  }

  fclose(fp);

  dfc_op->n_servers = n_servers;

  return dfc_op;
}

char *read_file(const char *fpath, ssize_t *bytes_read) {
  char *out_buf;
  int fd;
  struct stat st;

  if ((fd = open(fpath, O_RDONLY)) == -1) {
    fprintf(stderr, "[%s] failed to open %s: %s\n", __func__, fpath, strerror(errno));
    return NULL;
  }

  if (stat(fpath, &st) < 0) {
    if (close(fd) == -1) {
      fprintf(stderr, "[%s] failed to stat %s: %s\n", __func__, fpath, strerror(errno));
      exit(EXIT_FAILURE);
    }
    return NULL;
  }

  if ((out_buf = alloc_buf(st.st_size)) == NULL) {
    fprintf(stderr, "[%s] out of memory\n", __func__);
    exit(EXIT_FAILURE);
  }

  if ((*bytes_read = read(fd, out_buf, st.st_size)) < (ssize_t)st.st_size) {
    if (close(fd) == -1) {
      fprintf(stderr, "[%s] failed to close %s: %s\n", __func__, fpath, strerror(errno));
      exit(EXIT_FAILURE);
    }

    free(out_buf);

    return NULL;
  }

  if (close(fd) == -1) {
    fprintf(stderr, "[%s] failed to close %s: %s\n", __func__, fpath, strerror(errno));
    exit(EXIT_FAILURE);
  }

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

char *realloc_buf(char *buf, size_t size) {
  char *tmp_buf;

  tmp_buf = realloc(buf, size);
  if (chk_alloc_err(tmp_buf, "realloc", __func__, __LINE__ - 1) == -1) {
    return NULL;
  }

  buf = tmp_buf;

  return buf;
}

char **split_file(char *file_contents, size_t *chunk_sizes, size_t n_chunks) {
  char **chunks;
  size_t offset;

  chunks = (char **)alloc_buf(sizeof(char *) * n_chunks);
  if (chk_alloc_err(chunks, "malloc", __func__, __LINE__ - 1) == -1) {
    fprintf(stderr, "[ERROR] out of memory\n");

    return NULL;
  }

  offset = 0;
  for (size_t i = 0; i < n_chunks; ++i) {
    if ((chunks[i] = alloc_buf(chunk_sizes[i])) == NULL) {
      fprintf(stderr, "[ERROR] out of memory\n");

      return NULL;
    }

    memcpy(chunks[i], file_contents + offset, chunk_sizes[i]);

    offset += chunk_sizes[i];
  }

  return chunks;
}

void print_header(DFCHeader *dfc_hdr) {
  fputs("DFCHeader {\n", stderr);
  fprintf(stderr, "  cmd: %s\n", dfc_hdr->cmd);
  fprintf(stderr, "  filename: %s\n", dfc_hdr->fname);
  fprintf(stderr, "  chunk_offset: %zu\n", dfc_hdr->chunk_offset);
  fprintf(stderr, "  file_offset: %zu\n", dfc_hdr->file_offset);
  fputs("}\n", stderr);
}
