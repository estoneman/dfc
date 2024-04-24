#include "dfc/async.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dfc/dfc.h"
#include "dfc/dfc_util.h"
#include "dfc/bloom_filter.h"

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
  unsigned short fname_hash, start_index;

  pthread_rwlock_rdlock(&conf_lock);
  fprintf(stderr, "[%s] acquired rdlock on 'conf lock'\n", __func__);

  // read input file
  if ((file_contents = read_file(dfc_op->filename, &len_file)) == NULL) {
    fprintf(stderr, "[ERROR] unable to read file %s\n", dfc_op->filename);

    return NULL;
  }

  chunk_sizes = malloc(sizeof(size_t) * dfc_op->dfc_config->n_servers);
  if (chk_alloc_err(chunk_sizes, "malloc", __func__, __LINE__ - 1) == -1) {
    fprintf(stderr, "[ERROR] out of memory\n");
    free(file_contents);

    exit(EXIT_FAILURE);
  }

  // split file
  get_chunk_sizes(len_file, dfc_op->dfc_config->n_servers, chunk_sizes);
  if ((chunks = split_file(file_contents, chunk_sizes,
                           dfc_op->dfc_config->n_servers)) == NULL) {
    fprintf(stderr, "[ERROR] unable to chunkify %s\n", dfc_op->filename);
    free(file_contents);

    exit(EXIT_FAILURE);
  }

  // get hash of filename
  fname_hash = hash_djb2(dfc_op->filename);
  start_index = fname_hash % dfc_op->dfc_config->n_servers;
  for (size_t i = 0, j; i < dfc_op->dfc_config->n_servers; ++i) { 
    j = (start_index + i) % dfc_op->dfc_config->n_servers;
    fprintf(stderr, "[%s] would have sent pieces %zu and %zu to server %s\n",
            __func__, i, (i + 1) % dfc_op->dfc_config->n_servers,
            dfc_op->dfc_config->servers[j]);
  }

  // free resources
  for (size_t i = 0; i < dfc_op->dfc_config->n_servers; ++i) {
    free(chunks[i]);
  }

  pthread_rwlock_unlock(&conf_lock);
  fprintf(stderr, "[%s] released rdlock on 'conf lock'\n", __func__);

  free(chunks);
  free(chunk_sizes);
  free(file_contents);

  return NULL;
}

void *read_config(void *arg) {
  DFCConfig *dfc_config = (DFCConfig *)arg;

  char line[CONF_MAXLINE + 1];
  FILE *fp;
  size_t n_cols, addr_offset, n_servers;

  pthread_rwlock_wrlock(&conf_lock);
  fprintf(stderr, "[%s] acquired wrlock on 'conf_lock'\n", __func__);
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
  fprintf(stderr, "[%s] released wrlock held by 'conf lock'\n", __func__);

  return NULL;
}
