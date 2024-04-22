#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_

#define HASH_LEN 65536

typedef struct {
  unsigned int *data;
  size_t size;
  size_t capacity;
} BloomFilter;

void add_bloom_filter(BloomFilter *, const char *);
int check_bloom_filter(BloomFilter *, const char *);
BloomFilter *create_bloom_filter(size_t);
void destroy_bloom_filter(BloomFilter *);
unsigned short double_hash(const char *);
unsigned short hash_djb2(const char *);
unsigned short hash_fnv1a(const char *);
void show_set_bits(BloomFilter *bf);
void set_bit(BloomFilter *, size_t);
int test_bit(BloomFilter *bf, size_t idx);

#endif  // BLOOM_FILTER_H_
