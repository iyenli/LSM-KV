#include "kvstore.h"
#include <stdint.h>
#include <ctime>
#include <string>
#include <stdlib.h>
#include <fstream>
#include <time.h>
#include <Windows.h>

#define SIZE 262144
#define OPTIME 10000
#define UNIT 1000000
using namespace std;

int main()
{
    printf("--------test begin--------\n");

    srand((unsigned int)time(NULL));

    LARGE_INTEGER nFreq;
    LARGE_INTEGER nBeginTime;
    LARGE_INTEGER nEndTime;
    double continueTime;

    vector<double> result;

    KVStore store("./data");
    fstream outfile("./output.txt", ios::out);

    for (uint64_t i = 0; i < OPTIME; ++i)
    {
        // Test part1: PUT 16384
        QueryPerformanceFrequency(&nFreq);
        QueryPerformanceCounter(&nBeginTime);
        store.put(i, string(SIZE, 'a'));
        QueryPerformanceCounter(&nEndTime);
        continueTime = (double)(nEndTime.QuadPart - nBeginTime.QuadPart) * UNIT / nFreq.QuadPart; //获得对应的时间值，单位为 us
        outfile << continueTime << endl;
    }

    return 0;
}
