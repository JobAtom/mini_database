#ifndef MINI_DATABASE_TABLE_H
#define MINI_DATABASE_TABLE_H
#include <vector>
#include <string>
#include "column.h"
#include "sql/statements.h"
#include <map>

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
    bool insertcheck(hsql::InsertStatement *stmt);
    bool insertSelect(hsql::InsertStatement * stmt, map<string, table*> &table_list);
    bool updatecheck(hsql::UpdateStatement *stmt);
    bool update(hsql::UpdateStatement *stmt);

    vector<pair<string, column*>> select(hsql::SelectStatement *stmt);

    int getRecordSize(){return recordSize;};
    int getTotalRecordSize() { return totalRecordSize;};
    vector<column*> table_cols;
    int getrowSize() {return rowSize;};
    column* getPrimaryKey(){return primaryKey;};
    string getName(){
        return filename;
    };
    string getabsName(){
        return absname;
    }
    int getRowlength(){return rowlength;};

    void setRowLength(int length) {rowlength = length; };

    void setRowSize(int length) {rowSize = length; };

private:
    int recordSize = 0;
    string absname;

    column* primaryKey = NULL;
    string filename;

    int rowlength = 0;
    int totalRecordSize = 0;
    int rowSize = 0;

};

#endif //MINI_DATABASE_TABLE_H