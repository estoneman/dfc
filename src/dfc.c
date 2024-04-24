#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dfc/dfc.h"
#include "dfc/async.h"
#include "dfc/bloom_filter.h"
#include "dfc/dfc_util.h"

DFCCommand dfc_cmds[] = {{.cmd = "get", .hash = 0},
                         {.cmd = "list", .hash = 0},
                         {.cmd = "put", .hash = 0}};

void print_cmd(DFCCommand dfc_cmd) {
  fprintf(stderr, "DFCCommand = {\n");
  fprintf(stderr, "  cmd = %s\n", dfc_cmd.cmd);
  fprintf(stderr, "  hash = %d\n", dfc_cmd.hash);
  fprintf(stderr, "}\n");
}

void print_cmds(void) {
  for (size_t i = 0; i < N_CMD_SUPP; ++i) {
    print_cmd(dfc_cmds[i]);
  }
}

void dfc_print_config(DFCConfig *dfc_config) {
  fprintf(stderr, "DFCConfig {\n  no. servers = %zu\n", dfc_config->n_servers);
  for (size_t i = 0; i < dfc_config->n_servers; ++i) {
    fprintf(stderr, "  server %zu: %s\n", i, dfc_config->servers[i]);
  }
  fputs("}\n", stderr);
}

void usage(const char *program) {
  fprintf(stderr, "usage: %s <command> [filename] ... [filename]\n", program);
  fprintf(stderr, "supported commands:\n");
  for (size_t i = 0; i < N_CMD_SUPP; ++i) {
    fprintf(stderr, "  - %s\n", dfc_cmds[i].cmd);
  }
}

int main(int argc, char *argv[]) {
  BloomFilter *bf;
  char cmd[SZ_ARG_MAX + 1];
  unsigned short cmd_hash;
  pthread_t read_conf_tid;
  pthread_t file_proc_tids[argc];
  size_t n_files;
  DFCConfig *dfc_config;
  DFCOperation dfc_op;

  if (argc < 2) {
    fprintf(stderr, "[ERROR] not enough arguments supplied\n");
    usage(argv[0]);

    return EXIT_FAILURE;
  }

  dfc_config = (DFCConfig *)malloc(sizeof(DFCConfig));
  if (chk_alloc_err(dfc_config, "malloc", __func__, __LINE__ - 1) == -1) {
    fprintf(stderr, "[ERROR] out of memory\n");

    return EXIT_FAILURE;
  }

  dfc_config->servers = (char **)malloc(sizeof(char *) * MAX_SERVERS);
  if (chk_alloc_err(dfc_config->servers, "malloc", __func__, __LINE__ - 1) ==
      -1) {
    fprintf(stderr, "[ERORR] out of memory\n");

    return EXIT_FAILURE;
  }

  for (size_t i = 0; i < MAX_SERVERS; ++i) {
    dfc_config->servers[i] = (char *)alloc_buf(CONF_MAXLINE);
    if (chk_alloc_err(dfc_config->servers[i], "malloc", __func__,
                      __LINE__ - 1) == -1) {
      fprintf(stderr, "[ERROR] out of memory\n");

      return EXIT_FAILURE;
    }
  }

  dfc_config->n_servers = 0;
  if (pthread_create(&read_conf_tid, NULL, read_config, dfc_config) < 0) {
    fprintf(stderr, "[ERROR] could not create thread: %s:%d\n", __func__,
            __LINE__ - 1);
    exit(EXIT_FAILURE);
  }

  // initialize bloom filter and hash table w/ supported commands
  bf = create_bloom_filter(HASH_LEN);
  for (size_t i = 0; i < N_CMD_SUPP; ++i) {
    // add to bloom filter (for validation)
    dfc_cmds[i].hash = double_hash(dfc_cmds[i].cmd);
    add_bloom_filter(bf, dfc_cmds[i].cmd);
  }

  // read command
  strncpy(cmd, argv[1], SZ_ARG_MAX);

  // validate command
  if (!check_bloom_filter(bf, cmd)) {
    fprintf(stderr, "[ERROR] invalid command: %s\n", cmd);
    usage(argv[0]);

    // free resources
    destroy_bloom_filter(bf);

    return EXIT_FAILURE;
  }

  // act on command (selected `hash_fnv1a` arbitrarily)
  cmd_hash = hash_fnv1a(cmd);
  n_files = argc - 2;

  if (cmd_hash == hash_fnv1a("get")) {
    if (n_files == 0) {
      fprintf(
          stderr,
          "[ERROR] invalid get invocation, requires at least one file path\n");
      usage(argv[0]);

      return EXIT_FAILURE;
    }

    for (size_t i = 2; i < (size_t)argc; ++i) {
      fprintf(stderr, "[INFO] getting file: %s\n", argv[i]);

      file_proc_tids[i] = i;
      if (pthread_create(&file_proc_tids[i], NULL, handle_get, &argv[i]) < 0) {
        fprintf(stderr, "[ERROR] could not create thread: %s:%d\n", __func__,
                __LINE__ - 1);
        exit(EXIT_FAILURE);
      }
    }
  } else if (cmd_hash == hash_fnv1a("put")) {
    if (n_files == 0) {
      fprintf(
          stderr,
          "[ERROR] invalid put invocation, requires at least one file path\n");
      usage(argv[0]);

      return EXIT_FAILURE;
    }

    for (size_t i = 2; i < (size_t)argc; ++i) {
      file_proc_tids[i] = i;
      dfc_op.filename = argv[i];
      dfc_op.dfc_config = dfc_config;

      if (pthread_create(&file_proc_tids[i], NULL, handle_put, &dfc_op) < 0) {
        fprintf(stderr, "[ERROR] could not create thread: %s:%d\n", __func__,
                __LINE__ - 1);
        exit(EXIT_FAILURE);
      }
    }
  } else {  // list
    fprintf(stderr, "[INFO] querying for files... \n");
  }

  // free resources
  pthread_join(read_conf_tid, NULL);
  for (size_t i = 2; i < (size_t)argc; ++i) {
    pthread_join(file_proc_tids[i], NULL);
  }

  destroy_bloom_filter(bf);
  free(dfc_config->servers);
  free(dfc_config);

  return EXIT_SUCCESS;
}
