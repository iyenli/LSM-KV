//6月 05 2021
#ifndef COMPARE_H
#define COMPARE_H

#include <string>
#include <sstream>
#include <stdlib.h>
#include "kvstore.h"
using namespace std;

bool level1_cmp(const string &a, const string &b);

// HAS changed: new file name from now on!
bool level2_cmp(const string &a, const string &b);

bool cache_cmp_timestamp_search(const cachenode *a,
                                const cachenode *b);

bool sstable_cmp_timestamp_search(const sstable &a,
                                  const sstable &b);

bool cache_cmp_compaction(const cachenode *a, const cachenode *b);

bool sstable_cmp_compaction(const sstable &a,
                            const sstable &b);

#endif
