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

skiptable::skiptable(const string &dir, uint64_t timestamp)
{
    directory = dir;
    currentSize = initSize;
    header = new STnode(0, "", maxHeight);
    timeStamp = timestamp;
    level = 0;
    amount = 0;
    srand((unsigned)time(NULL));
}

skiptable::~skiptable()
{
    toDisk();
    clear();
    delete header;
}

cachenode *skiptable::put(uint64_t key,
                          const string &s)
{
    cachenode *ret = NULL;
    unsigned int newlen = naviSize + s.size();

    // if over the max size;
    if (currentSize + newlen > maxSize)
    {
        ret = toDisk();
        clear();
    }

    // search begins
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
    key = 0;
    return 1;
}

// reset every part in memory.
void skiptable::reset()
{
    clear();
    timeStamp = 0;
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
    //if no node in memtable, don't write in disk.
    STnode *cursor = header->forward[0];
    if (cursor == NULL)
        return NULL;

    cachenode *newCache = new cachenode;
    char *buffer = new char[currentSize + 1];
    buffer[currentSize] = '\0';

    // file path and name
    newCache->level = 0;
    newCache->amount = amount;
    newCache->timestamp = timeStamp;
    newCache->min_val = cursor->key;
    newCache->dir = directory;

    uint32_t offset = initSize + 12 * amount;
    char *key_offset_buffer = buffer + initSize;

    for (uint64_t i = 0; i < amount; ++i)
    {
        uint64_t key = cursor->key;

        newCache->bloomset->add(key);
        newCache->key_offset.push_back(keyO(key, offset));

        *(uint64_t *)(key_offset_buffer) = key;
        *(uint32_t *)(key_offset_buffer + 8) = offset;
        key_offset_buffer += 12;

        strncpy(buffer + offset, cursor->value.c_str(),
                cursor->value.size());

        offset += cursor->value.size();
        if (i == amount - 1)
            newCache->max_val = cursor->key;
        cursor = cursor->forward[0];
    }
    newCache->generate_header(buffer);

    // open file in binary mode
    fstream fs;
    string path = directory + newCache->generate_level() +
                  newCache->generate_name();
    fs.open(path, fstream::binary | fstream::out | fstream::trunc);

    fs.write(buffer, currentSize);
    //close file
    fs.close();
    delete[] buffer;
    return newCache;
}

void skiptable::init(string dir, uint64_t stamp)
{
    timeStamp = stamp + 1;
    directory = dir;
}
