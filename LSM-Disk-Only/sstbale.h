#ifndef SSTABLE_H
#define SSTABLE_H

#include "bloom.h"
#include "tool.h"
#include <fstream>
#include <iostream>
#include <string.h>
#include <string>
#include <vector>
#include <list>
class cachenode;
// sstable save, store and merge; A strong class to handle
// most tricky operations
class sstable
{
public:
    // head
    uint64_t min_val;
    uint64_t max_val;
    uint64_t amount;
    uint64_t timestamp;

    // bloom filter
    BloomFilter *bloomset;

    // key-value, use list instead of array to avoid
    // handling index
    list<keyV> key_value;

    // like "./data", to generate corresponding file
    // name anywhere, any time.
    string sstDir;
    int level;

public:
    sstable(string dir);
    sstable(cachenode *cache);
    sstable();
    ~sstable() {}
    string search(uint32_t offset,
                  uint32_t next_offset);

    // merge:
    static sstable sst_merge(sstable &a, sstable &b,
                             bool delFlag);
    static void sst_merge_scope(vector<sstable> &sstables,
                                bool delFlag);

    // save:
    cachenode *save_sst(const uint64_t &level,
                        const string &dir,
                        const uint64_t &size);
    vector<cachenode *> save_sst_scope(int level);

    // tool:
    string generate_name();
};

// 内存中的缓存 head + bloom filter and key-offset
class cachenode
{
private:
    // tool: search in a scope
    int search(const uint64_t &key, int start, int end);

public:
    // head
    uint64_t min_val;
    uint64_t max_val;
    uint64_t amount;
    uint64_t timestamp;

    // bloom filter
    BloomFilter *bloomset;

    // key-offset
    vector<keyO> key_offset;

    // 2 data to generate accurate loaction of cached file
    uint64_t level;
    string dir;

    // sstable to read data
    sstable *stable;

    cachenode(const string &dir);
    cachenode(const string &dir, uint64_t key);
    cachenode();
    ~cachenode();

    // usage: dir + "/" + level + generate();
    string generate_name();
    string generate_level();
    void generate_header(char *target);

    // in cache, cache node takes responsiblity to
    // get offset of given key
    int get_offset(const uint64_t &key);
};
#endif // SSTABLE_H
