//edited on: 4月 29 日 2021
//edited by LEE
#include "sstable.h"

keyO::keyO(uint64_t key, uint32_t offset)
{
    mem[0] = key;
    mem[1] = key >> 32;
    mem[2] = offset;
}

uint64_t keyO::getkey()
{
    uint64_t left = (mem[1]);
    left = left << 32;
    uint64_t right = (mem[0]);
    uint64_t ret = (right | left);
    return ret;
}

cachenode::cachenode()
{
    key_offset = NULL;
    bloomset = new bloom;
    min_val = UINT64_MAX;
    max_val = 0;
    amount = 0;
    timestamp = 0;
    currpointer = 0;
    level = 0;
}

// generate as "/level_4"
string cachenode::generate_level()
{
    return ("/level_" + int2String(level));
}

// generate as "/timestamp_minval.sst"
string cachenode::generate_name()
{
    return ("/" + int2String(timestamp) + "_" +
            int2String(min_val) + ".sst");
}

char *cachenode::generate_header()
{
    char *buffer = new char[initSize + 12 * amount];
    *(uint64_t *)(buffer) = timestamp;
    *(uint64_t *)(buffer + 8) = amount;
    *(uint64_t *)(buffer + 16) = min_val;
    *(uint64_t *)(buffer + 24) = max_val;
    strncpy(buffer + 32, bloomset->bits, 10240);
    strncpy(buffer + initSize, (char *)key_offset, 12 * amount);
    return buffer;
}

sstable::sstable(string ds)
{
    sstname = ds;
    // open file and get value
    fs.open(sstname, fstream::binary | fstream::in);
}

sstable::~sstable()
{
    fs.close();
}

string sstable::search(uint32_t offset, uint32_t next_offset)
{
    string ret;

    // the length
    fs.seekp(0, fs.end);
    uint64_t len = fs.tellg();

    // set the pointer to the first beginning
    fs.seekp(offset, fs.beg);
    next_offset = next_offset == 0
                      ? len - offset // len - offset vertified!
                      : next_offset - offset;
    char *buffer = new char[next_offset + 1];
    buffer[next_offset] = '\0';
    fs.read(buffer, next_offset);
    ret = string(buffer);
    delete[] buffer;
    return ret;
}
