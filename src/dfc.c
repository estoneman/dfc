#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dfc/bloom_filter.h"
#include "dfc/dfc_util.h"
#include "dfc/async.h"
#include "dfc/sk_util.h"
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

  int sockfds[dfc_op->n_servers];
  fill_sk_set(dfc_op, sockfds);
  if (cmd_hash == hash_djb2("get")) {
    if (argc == 0) {
      fprintf(stderr, "[ERROR] Expected files\n");

      return EXIT_FAILURE;
    }

    strncpy(dfc_op->fname, argv[0], PATH_MAX);

    GetOperation get_op;
    // copy filename into DFCOperation structure, if readable and exists
    if (adjacent_failure(sockfds, dfc_op->n_servers)) {
      fprintf(stderr, "[%s] get %s failed \n", __func__, dfc_op->fname);
      for (size_t i = 0; i < dfc_op->n_servers; ++i) {
        if (sockfds[i] > 0 && close(sockfds[i]) == -1) {  // close connected sockets, if any
          fprintf(stderr, "[%s] failed to close sfd=%d: %s\n", __func__,
                  sockfds[i], strerror(errno));
        }
      }

      return -1;
    }

    if ((get_op.sockfds = malloc(sizeof(int) * dfc_op->n_servers)) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    for (size_t j = 0; j < dfc_op->n_servers; ++j) {
      if (sockfds[j] > 0) {
        get_op.sockfds[j] = sockfds[j];
        set_timeout(get_op.sockfds[j], RCVTIMEO_SEC, RCVTIMEO_USEC);
      }
    }

    get_op.n_servers = dfc_op->n_servers;
    strncpy(get_op.fname, dfc_op->fname, PATH_MAX);

    if (pthread_create(&handler_tid, NULL, get_handle, &get_op) < 0) {
      fprintf(stderr, "[ERROR] could not create thread\n");
      exit(EXIT_FAILURE);
    }

    pthread_join(handler_tid, NULL);

    // free(get_op.sockfds);
  } else if (cmd_hash == hash_djb2("put")) {
    if (argc == 0) {
      fprintf(stderr, "[ERROR] Expected files\n");

      return EXIT_FAILURE;
    }

    strncpy(dfc_op->fname, argv[0], PATH_MAX);

    PutOperation put_op;

    if (access(argv[0], R_OK) == -1) {
      if (errno == ENOENT) {
        fprintf(stderr, "[ERROR] file %s not found\n", argv[0]);
      } else if (errno == EACCES) {
        fprintf(stderr, "[ERROR] file %s not readable\n", argv[0]);
      } else {
        perror("[ERROR]");
      }

      return -1;
    }

    if (adjacent_failure(sockfds, dfc_op->n_servers)) {
      fprintf(stderr, "[%s] put %s failed \n", __func__, dfc_op->fname);
      for (size_t j = 0; j < dfc_op->n_servers; ++j) {
        if (sockfds[j] > 0 && close(sockfds[j]) == -1) {  // close connected sockets, if any
          fprintf(stderr, "[%s] failed to close sfd=%d: %s\n", __func__,
                  sockfds[j], strerror(errno));
        }
      }
      return -1;
   }

    if ((put_op.sockfds = malloc(sizeof(int) * dfc_op->n_servers)) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    for (size_t j = 0; j < dfc_op->n_servers; ++j) {
      if (sockfds[j] > 0) {
        put_op.sockfds[j] = sockfds[j];
        set_timeout(put_op.sockfds[j], RCVTIMEO_SEC, RCVTIMEO_USEC);
      }
    }

    put_op.n_servers = dfc_op->n_servers;
    strncpy(put_op.fname, argv[0], PATH_MAX);
    if (pthread_create(&handler_tid, NULL, put_handle, &put_op) < 0) {
      fprintf(stderr, "[ERROR] could not create thread\n");
      exit(EXIT_FAILURE);
    }

    pthread_join(handler_tid, NULL);

    free(put_op.sockfds);
  } else {  // list
    int *list_fd;

    if ((list_fd = malloc(sizeof(int))) == NULL) {
      fprintf(stderr, "[FATAL] out of memory\n");
      exit(EXIT_FAILURE);
    }

    if (adjacent_failure(sockfds, dfc_op->n_servers)) {
      fprintf(stderr, "[%s] put %s failed \n", __func__, dfc_op->fname);
      for (size_t j = 0; j < dfc_op->n_servers; ++j) {
        if (sockfds[j] > 0 && close(sockfds[j]) == -1) {  // close connected sockets, if any
          fprintf(stderr, "[%s] failed to close sfd=%d: %s\n", __func__,
                  sockfds[j], strerror(errno));
        }
      }

      free(list_fd);
      return -1;
    }

    // only need a single connection
    for (size_t i = 0; i < dfc_op->n_servers; ++i) {
      if (sockfds[i] > 0) {
        *list_fd = sockfds[i];
        set_timeout(*list_fd, RCVTIMEO_SEC, RCVTIMEO_USEC);
        break;
      }
    }

    if (pthread_create(&handler_tid, NULL, list_handle, list_fd) < 0) {
      fprintf(stderr, "[ERROR] could not create thread\n");
      exit(EXIT_FAILURE);
    }

    pthread_join(handler_tid, NULL);

    free(list_fd);
  }

  for (size_t i = 0; i < dfc_op->n_servers; ++i) {
    if (sockfds[i] > 0 && close(sockfds[i]) == -1) {
      fprintf(stderr, "[%s] failed to close sfd=%d: %s\n", __func__, sockfds[i], strerror(errno));
      exit(EXIT_FAILURE);
    }
    fprintf(stderr, "[%s] close sfd=%d success\n", __func__, sockfds[i]);
  }

  for (size_t i = 0; i < MAX_SERVERS; ++i) {
    free(dfc_op->servers[i]);
  }
  free(dfc_op->servers);

  free(dfc_op);

  return 0;
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
    destroy_bloom_filter(bf);
    exit(EXIT_FAILURE);
  }

  destroy_bloom_filter(bf);
  return EXIT_SUCCESS;
}
