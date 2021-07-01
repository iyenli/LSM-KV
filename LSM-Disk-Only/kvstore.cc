#include "kvstore.h"

/*
* read exist sstable and set new timestamp for memtable
**/
KVStore::KVStore(const string &dir) : KVStoreAPI(dir)
{
    directory = dir;
    uint64_t timestamp = 0;
    if (utils::dirExists(directory))
    {
        int first_level_amount = utils::scanDir(directory, first_dir);

        // if some sstables exist
        if (first_level_amount > 0)
        {
            // make dir[0] = level0
            sort(first_dir.begin(), first_dir.end(), level1_cmp);
            for (int i = 0; i < first_level_amount; ++i)
            {
                string levelName = first_dir[i];
                string levelDir = directory + "/" + levelName;

                cacheptr.push_back(vector<cachenode *>());

                vector<string> second_dir;
                int second_level_num = utils::scanDir(levelDir, second_dir);

                for (int j = 0; j < second_level_num; ++j)
                {
                    cachenode *new_cache = new cachenode(levelDir + "/" + second_dir[j]);
                    new_cache->dir = directory;
                    new_cache->level = i;
                    cacheptr[i].push_back(new_cache);
                    timestamp = new_cache->timestamp > timestamp
                                    ? new_cache->timestamp
                                    : timestamp;
                }
                // make big timestamp in front
                sort(cacheptr[i].begin(), cacheptr[i].end(), cache_cmp_timestamp_search);
            }
        }
        else
        {
            utils::mkdir((directory + "/level_0").c_str());
            cacheptr.push_back(vector<cachenode *>());
        }
    }
    else
    {
        utils::mkdir(directory.c_str());
        utils::mkdir((directory + "/level_0").c_str());
        cacheptr.push_back(vector<cachenode *>());
    }
    timestamp++;
    memtable = new skiptable(directory, timestamp);
}

KVStore::~KVStore()
{
    memtable->toDisk();
    delete memtable;
    compaction(0);
    for (auto iter1 = cacheptr.begin(); iter1 != cacheptr.end(); ++iter1)
        for (auto iter2 = (*iter1).begin(); iter2 != (*iter1).end(); ++iter2)
            delete (*iter2);
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const string &s)
{
    cachenode *new_cache;
    if ((new_cache = memtable->put(key, s)) != NULL)
    {
        cacheptr[0].push_back(new_cache);
        compaction(0);
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
string KVStore::get(uint64_t key)
{
    string val = memtable->get(key);
    if (val == "")
    {
        // DIFFERENCES with Release version: get access to Disk directly
        first_dir.resize(0);
        first_level_amount = utils::scanDir(directory, first_dir);
        int second_level_amount;
        vector<string> second_dir;
        cachenode *cache;
        string path;
        for (int i = 0; i < first_level_amount; ++i)
        {
            second_dir.resize(0);
            second_level_amount =
                utils::scanDir(directory + "/" + first_dir[i],
                               second_dir);
            for (int j = 0; j < second_level_amount; ++j)
            {
                path = directory + "/" + first_dir[i] +
                       "/" + second_dir[j];
                cache = new cachenode(path, key);
                // keep searching in Disk
                if (cache->key_offset.size() == 0)
                {
                    delete cache;
                    continue;
                }
                else
                {
                    int index = cache->get_offset(key);
                    if (index < 0)
                    {
                        delete cache;
                        continue;
                    }
                    else
                    {
                        ifstream fs(path, ios::binary);
                        uint32_t offset = cache->key_offset[index].offset;
                        fs.seekg(offset);
                        if (index == cache->amount - 1)
                            fs >> val;
                        else
                        {
                            uint32_t nextOffset = (cache->key_offset)[index + 1].offset;
                            uint64_t len = nextOffset - offset;
                            char *result = new char[len + 1];
                            result[len] = '\0';
                            fs.read(result, len);
                            val = result;
                            delete[] result;
                        }
                        fs.close();
                        delete cache;
                        if (val == DELSIG)
                            return "";
                        return val;
                    }
                }
            }
        }
    }
    if (val == DELSIG)
        return "";
    return val;
}

/**
 * Delete the given key-value pair if iter exists.
 * Returns false if the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if (get(key) == "")
        return false;
    put(key, DELSIG);
    return 1;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    memtable->reset();

    // clear all .sst tables and all cache
    clear();
    cacheptr.push_back(vector<cachenode *>());
}

/**
 * compaction function
 * 1. conduct with THIS level and next level
 * 2. first_dirname and second_file_name all sequential!(level 0 - level 7 - level 10)
 * 		or (1.sst - 5.sst - 10.sst)
 * 3. cacheptr is as the same sequential way
 */
void KVStore::compaction(int level)
{
    unsigned long long max_file_number = pow(2, level + 1);
    if (cacheptr[level].size() <= max_file_number)
    {
        sort(cacheptr[level].begin(), cacheptr[level].end(), cache_cmp_timestamp_search);
        return;
    }

    long over = cacheptr[level].size() - max_file_number;
    // this and next level, table list we handled.
    vector<sstable> tables;
    uint64_t minkey = UINT64_MAX, maxkey = 0;
    // compact all SSTables in level-0
    if (level == 0)
    {
        for (auto iter = cacheptr[level].begin();
             iter != cacheptr[level].end(); ++iter)
        {
            maxkey = (*iter)->max_val > maxkey
                         ? (*iter)->max_val
                         : maxkey;
            minkey = (*iter)->min_val < minkey
                         ? (*iter)->min_val
                         : minkey;
            tables.push_back(sstable(*iter));
        }
        cacheptr[level].clear();
    }
    else
    {
        // now sort into compaction mode, then retrive
        sort(cacheptr[level].begin(), cacheptr[level].end(), cache_cmp_compaction);
        for (auto iter = cacheptr[level].begin();
             iter < cacheptr[level].begin() + over; ++iter)
        {
            maxkey = (*iter)->max_val > maxkey ? (*iter)->max_val : maxkey;
            minkey = (*iter)->min_val > minkey ? (*iter)->min_val : minkey;
            tables.push_back(sstable(*iter));
        }
        cacheptr[level].erase(cacheptr[level].begin(), cacheptr[level].begin() + over);
    }
    // past level switch to SEARCH Mode!
    sort(cacheptr[level].begin(), cacheptr[level].end(), cache_cmp_timestamp_search);

    // handle next level, flag: DELETE "DELETE SIGNAL"?
    bool delFlag = false;
    ++level;
    // next level exist, we found search scope according to max/min key
    if (level < cacheptr.size())
    {
        for (auto iter = cacheptr[level].begin();
             iter != cacheptr[level].end();)
            if (((*iter)->max_val <= maxkey && (*iter)->max_val >= minkey) ||
                ((*iter)->min_val <= maxkey && (*iter)->min_val >= minkey))
            {
                tables.push_back(sstable(*iter));
                iter = cacheptr[level].erase(iter);
            }
            else
                ++iter;
        if (level == cacheptr.size() - 1)
            delFlag = 1;
    }
    else
    {
        utils::mkdir((directory + "/level_" + int2String(level)).c_str());
        cacheptr.push_back(vector<cachenode *>());
    }

    // use rapid class method to merge two files
    for (auto iter = tables.begin(); iter != tables.end(); ++iter)
        utils::rmfile((iter->sstDir + iter->generate_name()).c_str());

    sort(tables.begin(), tables.end(), sstable_cmp_timestamp_search);

    // merge to tables[0], if delFlag == 1, delete all DELSIG in bottom layer.
    sstable::sst_merge_scope(tables, delFlag);

    // save to new level
    tables[0].sstDir = directory;
    vector<cachenode *> newCaches = tables[0].save_sst_scope(level);
    for (auto iter = newCaches.begin(); iter != newCaches.end(); ++iter)
        cacheptr[level].push_back(*iter);

    compaction(level);
}

/**
 * clear the kvstore cacheptr
 */
void KVStore::clear()
{
    // destruct timestamp & amount & cacheptr
    for (auto iter1 = cacheptr.begin(); iter1 != cacheptr.end(); ++iter1)
        for (auto iter2 = (*iter1).begin(); iter2 != (*iter1).end(); ++iter2)
        {
            utils::rmfile(((*iter2)->dir +
                           (*iter2)->generate_level() + (*iter2)->generate_name())
                              .c_str());
            if (iter2 + 1 == (*iter1).end())
                utils::rmdir(((*iter2)->dir + (*iter2)->generate_level()).c_str());
            delete (*iter2);
        }
    cacheptr.clear();
}
