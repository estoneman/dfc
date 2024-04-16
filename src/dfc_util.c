#include <dfc_util.h>

void get_chunks(size_t file_size, size_t n_chunks, size_t *out) {
  // === BEGIN FAILURE MODES ===
  if (out == NULL) {
    fprintf(stderr, "[ERROR] 'out' has not been allocated.. exiting\n");

    exit(EXIT_FAILURE);
  }

  if (file_size == 0 || n_chunks == 0 || n_chunks > file_size) {
    fprintf(stderr, "[ERROR] cannot partition file.. exiting\n");

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
  }

  size_t i;
  size_t sum;

  sum = 0;
  for (i = 0; i < n_chunks - 1; ++i) {
    out[i] = file_size / n_chunks;
    sum += out[i];
  }

  out[i] = file_size - sum;
}
