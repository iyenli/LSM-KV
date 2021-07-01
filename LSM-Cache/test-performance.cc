#include "kvstore.h"
#include <stdint.h>
#include <ctime>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <Windows.h>

#define OP 16384
#define PTIME 3
#define UNIT 1000000
using namespace std;

// for with cache版本的
int main()
{
    printf("--------test begin--------\n");
    // since ctime's precision can't satisfy the needs of measuring,
    // use Windows.h's high precision time measurement function.

    //QueryPerformanceCounter()这个函数返回高精确度性能计数器的当前计数值
    //QueryPerformanceFrequency()返回机器内部定时器的时钟频率,每秒嘀哒声的个数
    // considering that average error of QueryPerformanceCounter ~ us, so the UNIT we use is us

    srand((unsigned int)time(NULL));

    LARGE_INTEGER nFreq;
    LARGE_INTEGER nBeginTime;
    LARGE_INTEGER nEndTime;
    double continueTime;

    vector<double> result;
    uint64_t Toinsert[OP * PTIME];
    uint64_t Tosearch[OP * PTIME];
    uint64_t Todelete[OP * PTIME];

    for (int i = 0; i < OP * PTIME; ++i)
    {
        int p = rand() * 2;
        Toinsert[i] = (p < 128 ? 128 : p);
        Tosearch[i] = (rand()) * PTIME;
        Todelete[i] = (rand()) * PTIME;
    }

    /*
     * 测试策略：
     * 模拟实际使用情况的随机性，常规测试的规则：
     * 1. key值从( 0 - 65536 ); 前半部分插满值，后半部分为空，插入总次数为32767
     * 2. string长度从 128 bytes - 65536 bytes
     * 3. 生成的随机查询共32767次，存在数组内后开始查询
     * 4. 生成的随机删除共32767次，存在数组内后开始删除
     * 
    * */
    KVStore store("./data");

    // Test part1: PUT 16384
    QueryPerformanceFrequency(&nFreq);
    QueryPerformanceCounter(&nBeginTime);

    for (uint64_t i = 0; i < OP * PTIME; ++i)
        store.put(i, string(Toinsert[i], 'a'));

    QueryPerformanceCounter(&nEndTime);

    continueTime = (double)(nEndTime.QuadPart - nBeginTime.QuadPart) * UNIT / nFreq.QuadPart; //获得对应的时间值，单位为 us
    result.push_back(continueTime);

    // Test part2: SEARCH 16384
    QueryPerformanceFrequency(&nFreq);
    QueryPerformanceCounter(&nBeginTime);

    for (uint64_t i = 0; i < OP * PTIME; ++i)
        store.get(Tosearch[i]);

    QueryPerformanceCounter(&nEndTime);

    continueTime = (double)(nEndTime.QuadPart - nBeginTime.QuadPart) * UNIT / nFreq.QuadPart; //获得对应的时间值，单位为 us
    result.push_back(continueTime);

    // Test part2: DELETE 16384
    QueryPerformanceFrequency(&nFreq);
    QueryPerformanceCounter(&nBeginTime);

    for (uint64_t i = 0; i < OP * PTIME; ++i)
        store.del(Todelete[i]);

    QueryPerformanceCounter(&nEndTime);

    continueTime = (double)(nEndTime.QuadPart - nBeginTime.QuadPart) * UNIT / nFreq.QuadPart; //获得对应的时间值，单位为 us
    result.push_back(continueTime);

    cout << "RESULT:" << endl
         << result[0] << endl
         << result[1] << endl
         << result[2] << endl;

    return 0;
}
