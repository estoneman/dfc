#include <dfc_util.h>

void usage(const char *program) {
  fprintf(stderr, "usage: %s <file-size> <n-chunks>\n", program);
}

int main(int argc, char *argv[]) {
  size_t file_size;
  size_t n_chunks;
  size_t chunks[MAX_CHUNKS];

  if (argc < 3) {
    fprintf(stderr, "[ERROR] not enough arguments supplied\n");
    usage(argv[0]);

    exit(EXIT_FAILURE);
  }

  if ((file_size = strtoul(argv[1], NULL, 10)) == 0) {
    if (errno == EINVAL) {
      fprintf(stderr, "[ERROR] invalid file size specified\n");
      usage(argv[0]);

      exit(EXIT_FAILURE);
    }

    if (errno == ERANGE) {
      fprintf(stderr, "[ERROR] overflow error\n");

      exit(EXIT_FAILURE);
    }
  }

  if ((n_chunks = strtoul(argv[2], NULL, 10)) == 0) {
    if (errno == EINVAL) {
      fprintf(stderr, "[ERROR] invalid number of chunks specified\n");
      usage(argv[0]);

      exit(EXIT_FAILURE);
    }

    if (errno == ERANGE) {
      fprintf(stderr, "[ERROR] overflow error\n");

      exit(EXIT_FAILURE);
    }
  }

  if (n_chunks > MAX_CHUNKS) {
    fprintf(stderr, "[ERROR] only %d chunks are allowed\n", MAX_CHUNKS);
    usage(argv[0]);

    exit(EXIT_FAILURE);
  }

  get_chunks(file_size, n_chunks, chunks);

  fputs("[ ", stderr);
  for (size_t i = 0; i < n_chunks; ++i) {
    if (i == n_chunks - 1) {
      fprintf(stderr, "%zu", chunks[i]);
      continue;
    }

    fprintf(stderr, "%zu, ", chunks[i]);
  }
  fputs(" ]\n", stderr);

  return EXIT_SUCCESS;
}
