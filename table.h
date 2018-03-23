#ifndef MINI_DATABASE_TABLE_H
#define MINI_DATABASE_TABLE_H
#include <vector>
#include <string>
#include "column.h"
#include "sql/statements.h"

using namespace std;


class table{
public:
    table();
    table(const string& name);
    virtual ~table();
    void addColumn(vector<column*> &cols);
    column* getColumn(const string &name);
    void setPrimaryKey(const string &name);
    bool insert(hsql::InsertStatement *stmt);

    vector<column*> table_cols;

    column* getPrimaryKey(){return primaryKey;};
    string getName(){
        return filename;
    };
    int getRowlength(){return rowlength;}


private:
    column* primaryKey = NULL;
    string filename;
    int rowlength = 0;




};

#endif //MINI_DATABASE_TABLE_H