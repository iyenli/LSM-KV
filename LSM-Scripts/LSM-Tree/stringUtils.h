//edited on: 4月 29 日 2021
//edited by LEE
//6月 05 2021
#ifndef STRINGUTILS_H
#define STRINGUTILS_H

#include <string>
#include <sstream>
#include <stdlib.h>
#include <sstream>
#include "kvstore.h"
using namespace std;

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

bool cache_cmp_timestamp(const cachenode *a, const cachenode *b)
{
    if (a->timestamp != b->timestamp)
        return a->timestamp < b->timestamp;
    return a->min_val < b->min_val;
}

bool cache_cmp_minval(const cachenode *a, const cachenode *b)
{
    return a->min_val < b->min_val;
}

// just for priority queue in compaction!
// never use it in ANY OTHER WHERE!
// key larger, priority smaller
// when key same, stamp lager, priority smaller
// when use, just pop all same key until last one!
bool cache_cmp_compaction(const cachenode *a, const cachenode *b)
{
    uint64_t leftkey, rightkey;
    leftkey = a->key_offset[a->currpointer].getkey();
    rightkey = b->key_offset[b->currpointer].getkey();
    if (leftkey != rightkey)
        return leftkey < rightkey;
    return a->timestamp < b->timestamp;
}

#endif