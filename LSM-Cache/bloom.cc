#include "bloom.h"

BloomFilter::BloomFilter()
{
    // reset to zero
    bits.reset();
}

BloomFilter::BloomFilter(char *mem)
{
    memcpy((char *)&bits, mem, BLOOMSIZE / 8);
}

void BloomFilter::add(const uint64_t &key)
{
    uint32_t ret[BLOOMLEN];
    MurmurHash3_x64_128(&key, sizeof(key), 1, &ret);
    for (int i = 0; i < BLOOMLEN; ++i)
    {
        bits.set(ret[i] % BLOOMSIZE);
    }
}

bool BloomFilter::contain(const uint64_t &key)
{
    uint32_t ret[BLOOMLEN];
    MurmurHash3_x64_128(&key, sizeof(key), 1, &ret);
    for (int i = 0; i < BLOOMLEN; ++i)
        if (!bits[ret[i] % BLOOMSIZE])
            return 0;
    return 1;
}

// save to buffer
void BloomFilter::save(char *filter)
{
    memcpy(filter, (char *)&bits, BLOOMSIZE / 8);
}
