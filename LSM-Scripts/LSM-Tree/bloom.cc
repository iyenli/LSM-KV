//edited on: 4月 17 日 2021
//edited by LEE
#include "bloom.h"
// tool function
bloom::bloom()
{
    memset(bits, 0, bloomSize);
}

void bloom::add(const uint64_t &key)
{
    uint32_t ret[BLOOMLEN];
    uint64_t one = 1;
    MurmurHash3_x64_128(&key, keyLen, 0, ret);
    for (int i = 0; i < BLOOMLEN; ++i)
    {
        uint32_t tmp = ret[i];
        // u32int > 81920, so:
        tmp %= BITNUMBER;
        // in which position in bits[]
        uint32_t vectorPos = tmp / 8;
        // in which bit in bits[position]
        tmp %= 8;
        bits[vectorPos] |= (one << tmp);
    }
}

bool bloom::Contain(const uint64_t &key) const
{
    uint32_t ret[BLOOMLEN];
    MurmurHash3_x64_128(&key, keyLen, 0, ret);
    uint32_t tmp, one = 1;
    for (int i = 0; i < BLOOMLEN; ++i)
    {
        tmp = ret[i];
        // u32int > 81920, so:
        tmp %= BITNUMBER;
        // in which position in bits[]
        uint32_t vectorPos = tmp / 8;

        // in which bit in bits[position]
        tmp %= 8;
        if ((bits[vectorPos] & (one << tmp)) == 0)
            return false;
    }
    return true;
}
