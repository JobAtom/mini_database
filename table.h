#ifndef MINI_DATABASE_TABLE_H
#define MINI_DATABASE_TABLE_H
#include <vector>
#include <string>
#include "column.h"

using namespace std;


class table{
public:
    table();
    table(const string& name);
    virtual ~table();




private:
    string filename;


};

#endif //MINI_DATABASE_TABLE_H