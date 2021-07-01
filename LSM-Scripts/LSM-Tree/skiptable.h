//4月 16 2021
#ifndef SKIPTABLE_H
#define SKIPTABLE_H

#include <cstdlib>
#include <cmath>
#include <stdint.h>
#include <string>
#include <ctime>
#include <fstream>
#include "sstable.h"
#include "bloom.h"
#include "constant.h"
#include "utils.h"

using namespace std;

class STnode
{
public:
    unsigned int key;
    string value;
    STnode **forward;
    STnode(const uint64_t &key,
           const string &value,
           const unsigned short &level);
    ~STnode();
};

// memtable
class skiptable
{
private:
    //max size: 2097152 bytes
    unsigned int currentSize;
    // head node
    STnode *header;
    // current k-v amount
    uint64_t amount;
    // height
    unsigned int level;
    // max height: 10 level
    // time stamp, which is set to
    // max stamp at the beginning.
    uint64_t timeStamp;
    // data directory
    string directory;

public:
    skiptable(const string &dir);
    ~skiptable();
    cachenode *put(uint64_t key, const std::string &s);
    string get(uint64_t key);
    bool del(uint64_t key);
    void reset();
    cachenode *toDisk();
    void clear();
    void init(string dir, uint64_t stamp);
};

#endif
