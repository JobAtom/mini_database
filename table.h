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
    void addColumn(vector<column*> &cols);
    column* getColumn(string name);

    string filename;
    vector<column*> table_cols;




private:

    int rowlength;




};

#endif //MINI_DATABASE_TABLE_H