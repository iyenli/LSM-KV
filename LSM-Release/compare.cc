//edited on: 6月 06 日 2021
//edited by LEE
#include "compare.h"

bool level1_cmp(const string &a, const string &b)
{
    // level_ 6chars
    string subA = a.substr(6), subB = b.substr(6);
    int intA = atoi(subA.c_str()), intB = atoi(subB.c_str());
    return intA < intB;
}

// HAS changed: new file name from now on!
bool level2_cmp(const string &a, const string &b)
{
    // time_min.sst
    string subA = a.substr(0, a.length() - 4), subB = b.substr(0, b.length() - 4);
    // where
    int i, j;
    i = subA.find_first_of('_');
    j = subB.find_first_of('_');

    string subA1 = subA.substr(0, i), subB1 = subB.substr(0, j);
    subA = subA.substr(i + 1), subB = subB.substr(j + 1);

    int intA = atoi(subA1.c_str()), intB = atoi(subB1.c_str());
    int intAmin = atoi(subA.c_str()), intBmin = atoi(subB.c_str());

    if (intA != intB)
        return intA < intB;
    return intAmin < intBmin;
}

bool cache_cmp_timestamp_search(const cachenode *a,
                                const cachenode *b)
{
    return a->timestamp > b->timestamp;
}

bool sstable_cmp_timestamp_search(const sstable &a,
                                  const sstable &b)
{
    return a.timestamp > b.timestamp;
}

bool cache_cmp_compaction(const cachenode *a, const cachenode *b)
{
    if (a->timestamp != b->timestamp)
        return a->timestamp < b->timestamp;
    return a->min_val < b->min_val;
}

bool sstable_cmp_compaction(const sstable &a,
                            const sstable &b)
{
    if (a.timestamp != b.timestamp)
        return a.timestamp < b.timestamp;
    return a.min_val < b.min_val;
}
