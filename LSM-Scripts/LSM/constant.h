//4月 26 2021
#ifndef CONSTANT_H
#define CONSTANT_H

#include <stdint.h>
#include <string>
#include <vector>
#include <string.h>
using namespace std;

// for bloom filter
#define BLOOMSIZE 81920
#define BLOOMLEN 4
// max height of tower
#define maxHeight 10
#define MiddleP 0.5
// max BYTE size
#define maxSize 2097152
#define MAX_TABLE_SIZE 2097152
// init BYTE us==size
#define initSize 10272
// node's init BYTE size
#define naviSize 12
// delete string
#define DELSIG "~DELETED~"

// useful data-struct
struct keyO
{
    uint64_t key;
    uint32_t offset;
    keyO(uint64_t key0 = 0, uint32_t offset0 = 0) : key(key0), offset(offset0) {}
};

struct keyV
{
    uint64_t key;
    string val;

    keyV(const uint64_t &key = 0, const string &value = "")
    {
        this->key = key;
        this->val = value;
    }
};

#endif
