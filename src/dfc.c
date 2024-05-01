#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dfc/bloom_filter.h"
#include "dfc/dfc_util.h"
#include "dfc/async.h"
#include "dfc/dfc.h"

DFCCommand dfc_cmds[] = {{.cmd = "get", .hash = 0},
                         {.cmd = "list", .hash = 0},
                         {.cmd = "put", .hash = 0}};

int run_handler(int argc, char *argv[]) {
  DFCOperation *dfc_op;
  unsigned int cmd_hash;
  pthread_t handler_tid;

  if ((dfc_op = read_config()) == NULL) {
    return -1;
  }

  cmd_hash = hash_djb2(argv[0]);
  argc -= 1;
  argv += 1;

  if ((dfc_op->files = (char **)malloc(sizeof(char *) * argc)) ==
      NULL) {
    fprintf(stderr, "[ERORR] out of memory\n");

    exit(EXIT_FAILURE);
  }

  dfc_op->n_files = 0;
  for (int i = 0; i < argc; ++i) {
    if ((dfc_op->files[i] = (char *)alloc_buf(PATH_MAX + 1)) == NULL) {
      fprintf(stderr, "[ERROR] out of memory\n");

      exit(EXIT_FAILURE);
    }
  }

  // copy filenames into DFCOperation structure, if readable and exist
  for (int i = 0; i < argc; ++i) {
    if (access(argv[i], R_OK) == -1) {
      if (errno == ENOENT) {
        fprintf(stderr, "[ERROR] %s not found, skipping\n", argv[i]);
      } else if (errno == EACCES) {
        fprintf(stderr, "[ERROR] %s not readable, skipping\n", argv[i]);
      } else {
        perror("[ERROR]");
      }

      continue;
    }

    strncpy(dfc_op->files[i], argv[i], PATH_MAX);
    dfc_op->n_files++;
  }

  if (cmd_hash == hash_djb2("get")) {
    if (dfc_op->n_files == 0) {
      fprintf(stderr, "[ERROR] Expected files\n");
      
      return EXIT_FAILURE;
    }
  } else if (cmd_hash == hash_djb2("put")) {
    if (dfc_op->n_files == 0) {
      fprintf(stderr, "[ERROR] Expected files\n");
      
      return EXIT_FAILURE;
    }

    if (pthread_create(&handler_tid, NULL, handle_put, &dfc_op) < 0) {
      fprintf(stderr, "[ERROR] unable to create 'put' thread\n");
      exit(EXIT_FAILURE);
    }
  } else {  // list
    fprintf(stderr, "[INFO] querying for files\n");
  }

  pthread_join(handler_tid, NULL);

  for(size_t i = 0; i < MAX_SERVERS; ++i) {
    free(dfc_op->servers[i]);
  }
  free(dfc_op->servers);

  for(size_t i = 0; i < dfc_op->n_files; ++i) {
    free(dfc_op->files[i]);
  }
  free(dfc_op->files);

  free(dfc_op);

  return 0;
}

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

void print_op(DFCOperation *dfc_op) {
  fprintf(stderr, "DFCOperation {\n");
  fprintf(stderr, "  filenames (%zu) {\n", dfc_op->n_files);
  for (size_t i = 0; i < dfc_op->n_files; ++i) {
    fprintf(stderr, "    %s\n", dfc_op->files[i]);
  }
  fputs("  }\n", stderr);
  fprintf(stderr, "  servers (%zu) {\n", dfc_op->n_servers);
  for (size_t i = 0; i < dfc_op->n_servers; ++i) {
    fprintf(stderr, "    %s\n", dfc_op->servers[i]);
  }
  fputs("  }\n", stderr);
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

  if (argc < 2) {
    fprintf(stderr, "[ERROR] not enough arguments supplied\n");
    usage(argv[0]);

    return EXIT_FAILURE;
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

  // shift cli args to access supplied cmd and filenames
  argv += 1;

  // given n filenames, run command on each of them
  if (run_handler(argc - 1, argv) != 0) {
    fprintf(stderr, "[ERROR] %s handler failed\n", cmd);
    destroy_bloom_filter(bf);

    exit(EXIT_FAILURE);
  }

  destroy_bloom_filter(bf);

  return EXIT_SUCCESS;
}

/*
int run_handler2(char **argv, unsigned int cmd_hash, size_t n_files) {
  DFCConfig *dfc_config;
  DFCOperation dfc_op[n_files];
  pthread_t file_proc_tids[n_files];

  if ((dfc_config = read_config()) == NULL) {
    fprintf(stderr, "[ERROR] failed to read config\n");

    return EXIT_FAILURE;
  }

  if (cmd_hash == hash_fnv1a("get")) {
    if (n_files == 0) {
      fprintf(
          stderr,
          "[ERROR] invalid get invocation, requires at least one file path\n");
      usage(argv[0]);

      return EXIT_FAILURE;
    }

    for (size_t i = 0; i < n_files; ++i) {
      strncpy(dfc_op[i].filename, argv[i], PATH_MAX);
      dfc_op[i].dfc_config = dfc_config;

      file_proc_tids[i] = i;
      if (pthread_create(&file_proc_tids[i], NULL, handle_get, &dfc_op[i]) <
          0) {
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

    for (size_t i = 0; i < n_files; ++i) {
      strncpy(dfc_op[i].filename, argv[i], PATH_MAX);
      dfc_op[i].dfc_config = dfc_config;

      if (pthread_create(&file_proc_tids[i], NULL, handle_put, &dfc_op[i]) <
          0) {
        fprintf(stderr, "[ERROR] could not create thread: %s:%d\n", __func__,
                __LINE__ - 1);
        exit(EXIT_FAILURE);
      }
    }
  } else {  // list
    fprintf(stderr, "[INFO] querying for files... \n");

    if (pthread_create(&file_proc_tids[0], NULL, handle_list, NULL) < 0) {
      fprintf(stderr, "[ERROR] could not create thread: %s:%d\n", __func__,
              __LINE__ - 2);
      exit(EXIT_FAILURE);
    }
  }

  // free resources
  for (size_t i = 0; i < n_files; ++i) {
    pthread_join(file_proc_tids[i], NULL);
  }

  for (size_t i = 0; i < dfc_config->n_servers; ++i) {
    free(dfc_config->servers[i]);
  }

  free(dfc_config->servers);
  free(dfc_config);

  return 0;
}

 */
