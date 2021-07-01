#include "sstbale.h"

using namespace std;

cachenode::cachenode()
{
    bloomset = new BloomFilter();
    min_val = UINT64_MAX;
    max_val = 0;
    amount = 0;
    timestamp = 0;
    level = 0;
}

cachenode::cachenode(const string &dir)
{
    std::ifstream fs(dir, ios::binary);

    // load from given file
    fs.read((char *)&timestamp, 8);
    fs.read((char *)&amount, 8);
    fs.read((char *)&min_val, 8);
    fs.read((char *)&max_val, 8);

    // load bloom filter
    char *filterBuf = new char[BLOOMSIZE / 8];
    fs.read(filterBuf, BLOOMSIZE / 8);
    bloomset = new BloomFilter(filterBuf);

    char *key_offset_buf = new char[amount * 12];
    fs.read(key_offset_buf, amount * 12);
    for (uint32_t i = 0; i < amount; ++i)
        key_offset.push_back(keyO(*(uint64_t *)(key_offset_buf + 12 * i),
                                  *(uint32_t *)(key_offset_buf + 12 * i + 8)));

    delete[] filterBuf;
    delete[] key_offset_buf;
    fs.close();
}

cachenode::~cachenode()
{
    delete bloomset;
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

void cachenode::generate_header(char *buffer)
{
    *(uint64_t *)(buffer) = timestamp;
    *(uint64_t *)(buffer + 8) = amount;
    *(uint64_t *)(buffer + 16) = min_val;
    *(uint64_t *)(buffer + 24) = max_val;
    bloomset->save((buffer + 32));
    return;
}

// if not exist in kvstore, return -1, else return index
int cachenode::get_offset(const uint64_t &key)
{
    if (bloomset->contain(key) == 0)
        return -1;
    return search(key, 0, key_offset.size() - 1);
}

// tool function: if strart>end, return -1; use int just for error callback
int cachenode::search(const uint64_t &key, int start,
                      int end)
{
    if (start > end)
        return -1;
    uint64_t mid = (start + end) / 2;
    if (key_offset[mid].key == key)
        return mid;
    if (key_offset[mid].key > key)
        return search(key, start, mid - 1);
    return search(key, mid + 1, end);
}

sstable::sstable(string dir)
{
    sstDir = dir;
}

// when merging: load from cachenode & fs
sstable::sstable(cachenode *cache)
{
    string path = cache->dir + cache->generate_level() +
                  cache->generate_name();
    ifstream fs(path.c_str(), ios::binary);
    if (!fs)
    {
        printf("Fail to open file %s", path.c_str());
        exit(-1);
    }
    // store head in sstable, attention that we don't need to
    // store key-offset and bloom filter, they are invalid when
    // merging 2 sstable
    sstDir = cache->dir;
    level = cache->level;
    timestamp = cache->timestamp;
    amount = cache->amount;
    min_val = cache->min_val;
    max_val = cache->max_val;

    // read val from fs directly and store in key_value
    // use list to avoid handling index
    fs.seekg(cache->key_offset[0].offset);
    for (uint64_t i = 0; i < amount; ++i)
    {
        uint64_t key = cache->key_offset[i].key;
        uint32_t offset = cache->key_offset[i].offset;

        string value;
        if (i == amount - 1)
            fs >> value;
        else
        {
            uint64_t length = cache->key_offset[i + 1].offset -
                              offset;
            char buffer[length + 1];
            buffer[length] = '\0';
            fs.read(buffer, length);
            value = buffer;
        }
        key_value.push_back(keyV(key, value));
    }
    // if merge, cache-node is useless
    delete cache;
}

// use in save, so initialize some necessary
sstable::sstable()
{
    amount = 0;
    key_value.resize(0);
}

// next may be most tricky op in Whole LSM tree:
// given an initialized sstable, save them(even if
// may save to several fss), given an array of sstable,
// merge sort and save them to disk
// I'll write 2 version of function in this stage:
// 1. sst_merge this will merge 2 sstables
// 2. sst_merge_scope this will merge an array of sstable recrusively
// 3. save_sst this function save an sstable to Disk
// 4. save_sst_scope this function call save_sst recursively

// merge sort, remember to free return val
sstable sstable::sst_merge(sstable &a, sstable &b, bool delFlag)
{
    // merge key-value into ret.key_value
    sstable ret;
    while ((!a.key_value.empty()) && (!b.key_value.empty()))
    {
        uint64_t keyA = a.key_value.front().key;
        uint64_t keyB = b.key_value.front().key;
        if (keyA < keyB)
        {
            ret.key_value.push_back(a.key_value.front());
            a.key_value.pop_front();
        }
        else if (keyA > keyB)
        {
            ret.key_value.push_back(b.key_value.front());
            b.key_value.pop_front();
        }
        else
        {
            // if we meet equal value, use larger timestamp one(a)
            ret.key_value.push_back(a.key_value.front());
            a.key_value.pop_front();
            b.key_value.pop_front();
        }
        if (delFlag && ret.key_value.back().val == DELSIG)
            ret.key_value.pop_back();
    }
    // handle last k-v pairs
    while (!a.key_value.empty())
    {
        keyV newKV = a.key_value.front();
        a.key_value.pop_front();
        if (!delFlag || newKV.val != DELSIG)
            ret.key_value.push_back(newKV);
    }
    while (!b.key_value.empty())
    {
        keyV newKV = b.key_value.front();
        b.key_value.pop_front();
        if (!delFlag || newKV.val != DELSIG)
            ret.key_value.push_back(newKV);
    }
    // use a's timestamp
    ret.timestamp = a.timestamp;
    return (ret);
}

void sstable::sst_merge_scope(vector<sstable> &sstables, bool delFlag)
{
    uint32_t size = sstables.size();
    if (size == 1)
        return;

    //  reduce half of them once time
    uint32_t mid = size / 2;
    std::vector<sstable> half;

    for (uint32_t i = 0; i < mid; ++i)
        half.push_back(sst_merge(sstables[2 * i], sstables[2 * i + 1], delFlag));
    if (size % 2)
        half.push_back(sstables[size - 1]);
    sst_merge_scope(half, delFlag);
    sstables = half;
}

// a correct, effective save funtion, return cachenode in memory
// to store in main-kvstore directly
cachenode *sstable::save_sst(const uint64_t &level,
                             const string &dir,
                             const uint64_t &size)
{
    cachenode *new_cache = new cachenode;
    char *buffer = new char[size];

    new_cache->timestamp = timestamp;
    new_cache->amount = amount;
    new_cache->min_val = key_value.front().key;
    new_cache->max_val = key_value.back().key;

    uint32_t offset = initSize + 12 * amount;
    char *key_offset_buf = buffer + initSize;

    for (auto iter = key_value.begin();
         iter != key_value.end(); ++iter)
    {
        new_cache->bloomset->add(iter->key);
        *(uint64_t *)key_offset_buf = iter->key;
        *(uint32_t *)(key_offset_buf + 8) = offset;

        new_cache->key_offset.push_back(keyO(iter->key, offset));

        uint32_t len = ((*iter).val).size();
        uint32_t newOffset = offset + len;

        memcpy(buffer + offset, ((*iter).val).c_str(), len);
        offset = newOffset;
        key_offset_buf += 12;
    }

    new_cache->generate_header(buffer);

    new_cache->dir = dir;
    new_cache->level = level;
    string pathname = dir + new_cache->generate_level() +
                      new_cache->generate_name();
    ofstream fs(pathname, ios::binary | ios::out);
    fs.write(buffer, size);
    delete[] buffer;

    fs.close();
    return new_cache;
}

vector<cachenode *> sstable::save_sst_scope(int level)
{
    std::vector<cachenode *> ret;

    // divide given Large kv-pairs list to 2048B size
    sstable table = sstable();
    uint64_t newsize, size = initSize;

    while (!key_value.empty())
    {
        keyV newKV = key_value.front();
        newsize = naviSize + newKV.val.size();
        if (size + newsize > maxSize)
        {
            table.sstDir = sstDir;
            table.timestamp = timestamp;
            ret.push_back(table.save_sst(level, sstDir, size));
            table = sstable();
            size = initSize;
        }
        size += newsize;
        table.key_value.push_back(newKV);
        key_value.pop_front();
        ++table.amount;
    }
    // last block can less than 2MB
    if (size > initSize)
    {
        table.timestamp = timestamp;
        ret.push_back(table.save_sst(level, sstDir, size));
    }
    return ret;
}

// generate as "/timestamp_minval.sst"
string sstable::generate_name()
{
    return ("/level_" + int2String(level) +
            "/" + int2String(timestamp) + "_" +
            int2String(min_val) + ".sst");
}
