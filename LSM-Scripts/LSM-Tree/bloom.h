//4月 17 2021
#ifndef BLOOM_H
#define BLOOM_H

#include <stdint.h>
#include <iostream>
#include <vector>
#include <string>
#include "MurmurHash3.h"
#include "utils.h"
#include "constant.h"

using namespace std;

class bloom
{
public:
    char bits[bloomSize];

    bloom();
    ~bloom();

    void add(const uint64_t &key);
    bool Contain(const uint64_t &key) const;
};

#endif
