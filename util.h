//
// Created by JinAobo on 3/23/18.
//

#ifndef MINI_DATABASE_UTIL_H
#define MINI_DATABASE_UTIL_H
#include <iostream>
#include <string>
#include <fstream>
#include <cstring>
#include <vector>
#include "table.h"
#include "util.h"
#include <iomanip>
#include <map>
using namespace std;


class util{
public:
    util();
    static table* getTable(const string &name, map<string, table*> table_list);
    static bool compareString(const std::string& str1, const std::string& str2);
    static bool compareString(const std::string& str1, const char* str2);
    static bool compareString(const char* str1, const char* str2);
    static int compareChar(const char* str1, const char* str2);
    static bool PrintRecords(hsql::SelectStatement *stmt, vector<pair<string, column*>> cols, table* t, int &selectvalue);
    static bool PrintJoinRecords(hsql::SelectStatement *stmt, vector<pair<string, column*>> colsleft, vector<pair<string, column*>> colsright, table* tleft, table* tright);


};

#endif //MINI_DATABASE_UTIL_H
