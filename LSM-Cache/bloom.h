//4月 27 2021
#ifndef BLOOM_H
#define BLOOM_H

using namespace std;
#include <bitset>
#include <stdint.h>
#include "constant.h"
#include "MurmurHash3.h"
#include <string.h>

class BloomFilter
{
private:
    bitset<BLOOMSIZE> bits;

public:
    BloomFilter();
    BloomFilter(char *mem);
    ~BloomFilter() {}
    void add(const uint64_t &key);
    bool contain(const uint64_t &key);
    void save(char *buf);

    bitset<BLOOMSIZE> *getSet() { return &bits; }
};

#endif
