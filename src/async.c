#include "dfc/async.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dfc/dfc.h"
#include "dfc/dfc_util.h"

pthread_rwlock_t conf_lock = PTHREAD_RWLOCK_INITIALIZER;

void *handle_get(void *arg) {
  char *filename = *(char **)arg;

  fprintf(stderr, "[INFO] put thread got %s\n", filename);

  return NULL;
}

void *handle_put(void *arg) {
  DFCOperation *dfc_op = (DFCOperation *)arg;

  char *file_contents;
  char **chunks;
  size_t *chunk_sizes;
  size_t len_file;

  pthread_rwlock_rdlock(&conf_lock);

  // read input file
  if ((file_contents = read_file(dfc_op->filename, &len_file)) == NULL) {
    fprintf(stderr, "[ERROR] unable to read file %s\n", dfc_op->filename);

    return NULL;
  }

  chunk_sizes = malloc(sizeof(size_t) * dfc_op->dfc_config.n_servers);
  if (chk_alloc_err(chunk_sizes, "malloc", __func__, __LINE__ - 1) == -1) {
    fprintf(stderr, "[ERROR] out of memory\n");
    free(file_contents);

    exit(EXIT_FAILURE);
  }

  // split file
  fprintf(stderr, "getting chunk sizes given:\n");
  fprintf(stderr, "  path to file: %s\n", dfc_op->filename);
  fprintf(stderr, "  length of file: %zu\n", len_file);
  fprintf(stderr, "  no. servers: %zu\n", dfc_op->dfc_config.n_servers);

  get_chunk_sizes(len_file, dfc_op->dfc_config.n_servers, chunk_sizes);
  if ((chunks = split_file(file_contents, chunk_sizes,
                           dfc_op->dfc_config.n_servers)) == NULL) {
    fprintf(stderr, "[ERROR] unable to chunkify %s\n", dfc_op->filename);
    free(file_contents);

    exit(EXIT_FAILURE);
  }

  for (size_t i = 0; i < dfc_op->dfc_config.n_servers; ++i) {
    fprintf(stderr, "\n\n[INFO] server: %s\n", dfc_op->dfc_config.servers[i]);
    fprintf(stderr, "       sending chunk %zu (size=%zu)\n\n", i,
            chunk_sizes[i]);
    fwrite(chunks[i], sizeof(char), chunk_sizes[i], stderr);
  }

  // free resources
  for (size_t i = 0; i < dfc_op->dfc_config.n_servers; ++i) {
    free(chunks[i]);
  }

  free(chunks);
  free(chunk_sizes);
  free(file_contents);

  pthread_rwlock_unlock(&conf_lock);

  return NULL;
}

void *read_config(void *arg) {
  DFCConfig *dfc_config = (DFCConfig *)arg;
  (void)dfc_config;

  char line[CONF_MAXLINE + 1];
  FILE *fp;
  size_t n_cols, addr_offset, n_servers;

  pthread_rwlock_wrlock(&conf_lock);
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

  dfc_config->n_servers = n_servers;
  pthread_rwlock_unlock(&conf_lock);

  return NULL;
}
