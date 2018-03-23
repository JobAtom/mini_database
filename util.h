//
// Created by JinAobo on 3/23/18.
//

#ifndef MINI_DATABASE_UTIL_H
#define MINI_DATABASE_UTIL_H
#include <iostream>
#include <string>
#include "table.h"
#include <map>
using namespace std;


class util{
public:
    util();
    static table* getTable(const string &name, map<string, table*> table_list);
    static bool compareString(const std::string& str1, const std::string& str2);
    static bool compareString(const std::string& str1, const char* str2);
    static bool compareString(const char* str1, const char* str2);
};

#endif //MINI_DATABASE_UTIL_H
