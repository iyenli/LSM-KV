//edited on: 4月 16 日 2021
//edited by LEE

#include "skiptable.h"

// some tool functions

// return the height of tower, ignore the bottom layer
unsigned int Ramdomlevel()
{
    unsigned int ret = 0;
    while (rand() < RAND_MAX * MiddleP && ret < maxHeight)
        ++ret;
    return ret;
}

STnode::STnode(const uint64_t &skey,
               const string &svalue, const unsigned short &slevel)
{
    key = skey;
    value = svalue;
    forward = new STnode *[slevel + 1];
    memset(forward, 0, sizeof(STnode *) * (slevel + 1));
}

STnode::~STnode()
{
    delete[] forward;
}

skiptable::skiptable(const string &dir)
{
    directory = dir;
    currentSize = initSize;
    header = new STnode(0, "", maxHeight);
    level = 0;
    amount = 0;
    srand((unsigned)time(NULL));
}

skiptable::~skiptable()
{
    clear();
    delete header;
}

cachenode *skiptable::put(uint64_t key,
                          const std::string &s)
{
    cachenode *ret = NULL;
    unsigned int newlen = naviSize + s.size();

    // if over the max size;
    if (currentSize + newlen > maxSize)
    {
        ret = toDisk();
        clear();
    }

    STnode *cursor = header;
    STnode *tmp[maxHeight + 1];
    memset(tmp, 0, sizeof(STnode *) * (maxHeight + 1));
    // find every layer's points to put new node in
    for (int i = level; i >= 0; i--)
    {
        while (cursor->forward[i] != NULL &&
               cursor->forward[i]->key < key)
            cursor = cursor->forward[i];
        tmp[i] = cursor;
    }
    cursor = cursor->forward[0];
    if (cursor != NULL && cursor->key == key)
    {
        currentSize += (s.size() - cursor->value.size());
        cursor->value = s;
        return ret;
    }
    else
    {
        unsigned int newLevel = Ramdomlevel();
        if (newLevel > level)
        {
            for (unsigned int i = level + 1; i <= newLevel; ++i)
                tmp[i] = header;
            level = newLevel;
        }
        cursor = new STnode(key, s, newLevel);
        for (unsigned int i = 0; i <= newLevel; i++)
        {
            cursor->forward[i] = tmp[i]->forward[i];
            tmp[i]->forward[i] = cursor;
        }
        currentSize += newlen;
        amount += 1;
    }
    return ret;
}

// get value in memtable
std::string skiptable::get(uint64_t key)
{
    STnode *cursor = header;
    for (int i = level; i >= 0; i--)
    {
        while (cursor->forward[i] != NULL &&
               cursor->forward[i]->key < key)
            cursor = cursor->forward[i];
    }
    cursor = cursor->forward[0];
    if (cursor != NULL && cursor->key == key)
        return cursor->value;
    return "";
}

// delete in memtable
bool skiptable::del(uint64_t key)
{
    bool flag;
    STnode *cursor = header;
    STnode *tmp[maxHeight + 1];
    memset(tmp, 0, sizeof(STnode *) * (maxHeight + 1));
    for (int i = level; i >= 0; i--)
    {
        while (cursor->forward[i] != NULL &&
               cursor->forward[i]->key < key)
            cursor = cursor->forward[i];
        tmp[i] = cursor;
    }
    cursor = cursor->forward[0];
    if (cursor != NULL && cursor->key == key && cursor->value != DELSIG)
    {
        unsigned int len = naviSize + cursor->value.length();
        currentSize -= len;
        amount -= 1;
        for (unsigned int i = 0; i <= level; i++)
        {
            if (tmp[i]->forward[i] != cursor)
                break;
            tmp[i]->forward[i] = cursor->forward[i];
        }
        delete cursor;
        while (level > 0 && header->forward[level] == NULL)
            level--;
        flag = true;
    }
    else
        flag = false;

    return flag;
}

// reset every part in memory.
void skiptable::reset()
{
    clear();
    delete header;
    currentSize = initSize;
    amount = 0;
    level = 0;
    timeStamp = 0;
    directory = "";
}

// clear memory
void skiptable::clear()
{
    STnode *tmp;
    while (header != NULL)
    {
        tmp = header;
        header = header->forward[0];
        delete tmp;
    }
    header = new STnode(0, "", maxHeight);
    ++timeStamp;
    currentSize = initSize;
    amount = 0;
    level = 0;
}

// memtable full || system terminates normally.
cachenode *skiptable::toDisk()
{
    char *buffer = new char[currentSize];
    //if no node in memtable, don't write in disk.
    STnode *cursor = header->forward[0];
    if (cursor == NULL)
        return NULL;

    cachenode *newCache = new cachenode();

    // file path and name
    newCache->level = 0;
    newCache->amount = amount;
    newCache->timestamp = timeStamp;
    newCache->min_val = cursor->key;

    // open file in binary mode
    string path = directory + newCache->generate_level() +
                  newCache->generate_name();
    fstream fs;
    fs.open(path, fstream::binary | fstream::out | fstream::trunc);

    newCache->key_offset = new keyO[amount];
    uint32_t offset = initSize + 12 * amount;
    for (uint64_t i = 0; i < amount; ++i)
    {
        uint64_t key = cursor->key;

        newCache->bloomset->add(key);
        newCache->key_offset[i] = keyO(key, offset);

        strncpy(buffer + offset, cursor->value.c_str(),
                cursor->value.size());
        offset += cursor->value.size();
        if (i == amount - 1)
            newCache->max_val = cursor->key;
        cursor = cursor->forward[0];
    }
    char *cacheptr = newCache->generate_header();
    strncpy(buffer, cacheptr, initSize + 12 * amount);
    delete[] cacheptr;

    fs.write(buffer, currentSize);
    //close file
    fs.close();
    return newCache;
}

void skiptable::init(string dir, uint64_t stamp)
{
    timeStamp = stamp + 1;
    directory = dir;
}
