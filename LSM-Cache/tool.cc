//edited on: 6月 06 日 2021
//edited by LEE
#include "tool.h"

string int2String(uint64_t target)
{
    stringstream transfer;
    transfer << target;
    return transfer.str();
}

string levelname(int level)
{
    stringstream transfer;
    transfer << level;
    return ("level_" + transfer.str());
}