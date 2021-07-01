#include "kvstore.h"
#include <stdint.h>
#include <ctime>
#include <string>
#include <Windows.h>

using namespace std;

int main()
{
    printf("--------test begin--------\n");
    // since ctime's precision can't satisfy the needs of measuring,
    // use Windows.h's high precision time measurement function.
    LARGE_INTEGER nFreq;
    LARGE_INTEGER nBeginTime;
    LARGE_INTEGER nEndTime;

    KVStore store("./data");
    QueryPerformanceFrequency(&nFreq);
    QueryPerformanceCounter(&nBeginTime);

    QueryPerformanceCounter(&nEndTime);

    int continueTime = (double)(nEndTime.QuadPart - nBeginTime.QuadPart) * 1000 / nFreq.QuadPart; //获得对应的时间值，单位为秒

    printf("time cost is %d ms.", continueTime);
    system("pause");
    return 0;

    for (uint64_t i = 0; i < 10000; ++i)
        store.put(i, string(100000, 's'));

    cout << s;
    return 0;
}
