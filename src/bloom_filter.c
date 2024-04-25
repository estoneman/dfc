#include <stdio.h>
#include <stdlib.h>

#include "dfc/dfc_util.h"
#include "dfc/bloom_filter.h"

void add_bloom_filter(BloomFilter *bf, const char *s) {
  unsigned short h1, h2;

  h1 = hash_djb2(s);
  h2 = hash_fnv1a(s);

  set_bit(bf, h1);
  set_bit(bf, h2);
}

int check_bloom_filter(BloomFilter *bf, const char *s) {
  unsigned short h1, h2;

  h1 = hash_djb2(s);
  h2 = hash_fnv1a(s);

  if (test_bit(bf, h1) && test_bit(bf, h2)) {
    return 1;
  }

  return 0;
}

BloomFilter *create_bloom_filter(size_t capacity) {
  BloomFilter *bf;
  size_t num_uints;

  bf = (BloomFilter *)malloc(sizeof(BloomFilter));
  if (chk_alloc_err(bf, "malloc", __func__, __LINE__ - 1) == -1) {
    return NULL;
  }

  num_uints = (capacity + UINT_SZ - 1) / UINT_SZ;

  bf->size = 0;
  bf->capacity = capacity;

  bf->data = (unsigned int *)calloc(num_uints, UINT_SZ);
  if (chk_alloc_err(bf->data, "calloc", __func__, __LINE__ - 1) == -1) {
    free(bf);

    return NULL;
  }

  return bf;
}

void destroy_bloom_filter(BloomFilter *bf) {
  if (bf != NULL) {
    free(bf->data);
    free(bf);
  }
}

unsigned short double_hash(const char *s) {
  return hash_djb2(s) & hash_fnv1a(s);
}

unsigned short hash_fnv1a(const char *s) {
  unsigned int hash = 2166136261u;
  int c;

  while ((c = *s)) {
    hash ^= (unsigned int)(*s++);
    hash *= 16777619u;
  }

  return (unsigned short)(hash >> 16) ^ (unsigned short)hash;
}

unsigned short hash_djb2(const char *s) {
  unsigned int hash = 5381;
  int c;

  while ((c = *s++)) {
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  }

  return (unsigned short)hash;
}

void show_set_bits(BloomFilter *bf) {
  for (size_t i = 0; i < bf->capacity; ++i) {
    if (test_bit(bf, i)) {
      fprintf(stderr, "[INFO] bit %zu is set\n", i);
      continue;
    }
  }
}

void set_bit(BloomFilter *bf, size_t idx) {
  if (idx >= bf->capacity) {
    return;
  }

  bf->data[idx / UINT_SZ] |= (1u << (idx % UINT_SZ));
  bf->size++;
}

int test_bit(BloomFilter *bf, size_t idx) {
  if (idx >= bf->capacity) {
    return 1;
  }

  return (bf->data[idx / UINT_SZ] & (1u << (idx % UINT_SZ))) != 0;
}
