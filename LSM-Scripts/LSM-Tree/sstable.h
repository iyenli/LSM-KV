//4月 29 2021
#ifndef SSTABLE_H
#define SSTABLE_H

#include "kvstore_api.h"
#include "bloom.h"
#include <fstream>
#include <iostream>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>
#include <list>
using namespace std;

// global tool: return string given a number
string int2String(uint64_t target)
{
    stringstream transfer;
    transfer << target;
    return transfer.str();
}

struct keyO
{
    uint32_t mem[3];

    keyO(uint64_t key, uint32_t offset);
    keyO() {}

    uint64_t getkey();
    uint32_t getoffset() { return mem[2]; }
};

struct kv_pair
{
    uint64_t key;
    string val;
};

// sstable save, store and merge
class sstable
{
private:
    string sstname;
    fstream fs;

public:
    sstable(string ds);
    ~sstable();
    string search(uint32_t offset,
                  uint32_t next_offset);
};

// 内存中的缓存
class cachenode
{
public:
    // head
    uint64_t min_val;
    uint64_t max_val;
    uint64_t amount;
    uint64_t timestamp;
    keyO *key_offset;
    bloom *bloomset;

    // special: only when compaction
    uint64_t currpointer;
    uint64_t level;

    // sstable to read data
    sstable *stable;

    cachenode();

    // usage: dir + "/" + level + generate();
    string generate_name();
    string generate_level();
    char *generate_header();
};

#endif
