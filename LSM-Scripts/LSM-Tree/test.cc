//edited on: 4月 27 日 2021
//edited by LEE
#include "utils.h"
#include <iostream>
#include <vector>

using namespace std;

int main()
{
    vector<string> c;
    cout << utils::scanDir("./data/level_0", c) << endl;
    for (int i = 0; i < c.size(); ++i)
        cout << c[i] << endl;
    return 0;
}