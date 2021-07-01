#pragma once
//4月 18 2021
#ifndef KVSTORE_H
#define KVSTORE_H

#include "kvstore_api.h"
#include "skiptable.h"
#include "compare.h"
#include "utils.h"
#include <cmath>
#include <iostream>
#include <queue>
#include <algorithm>
#include <fstream>

using namespace std;

class KVStore : public KVStoreAPI
{
    // You can add your implementation here
private:
    // memory table
    skiptable *memtable;


    // link list in the memory,store dir
    string directory;

    // total level amount
    int first_level_amount;

    // first level dir name as "level_0"
    vector<string> first_dir;

    // cachenode
    vector<vector<cachenode *>> cacheptr;

public:
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s);

    std::string get(uint64_t key);

    bool del(uint64_t key);

    void reset();

    void compaction(int level);

    void clear();
};

#endif
