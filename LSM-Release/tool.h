#ifndef TOOL_H
#define TOOL_H

#include <stdint.h>
#include <sstream>
#include <string>
using namespace std;

// global tool: return string given a number
string int2String(uint64_t target);
string levelname(int level);

#endif // TOOL_H
