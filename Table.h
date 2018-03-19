#ifndef TABLE_H
#define TABLE_H
#include <vector>
#include <string>
#include "defines.h"
#include "sql/statements.h"

namespace dbms {

struct Column{

    enum DataType {
        INT = 1,
        CHAR = 2,
    };

    Column(const std::string& name, DataType type, int size);
    Column();

    std::string toString();
    void fromString(const std::string& str);

    std::string name;
    int size;
    int trueSize;
    DataType type;
    int offset = 0;
    std::string tableName;

    Column* clone();
    bool compareName(hsql::Expr*);
};

class Table
{
public:
    Table();
    Table(const std::string& name);
    virtual ~Table();

    Column* getColumn(const std::string& colName);
    Column* getColumn(hsql::Expr* expr);
    void setColumns(const std::vector<Column*>& value, bool changeTableName = true);
    void setPrimaryKey(const std::string& colName);
    bool createFile();
    bool removeFile();
    bool insert(hsql::InsertStatement *stmt);
    bool select(hsql::SelectStatement *stmt, Table* dstTable = NULL, hsql::InsertStatement* insertStmt = NULL);
    bool join(Column* leftCol, Table* rightTable, Column* rightCol, Table* dstTable);

    std::string getName(){return name;}
    std::string getFileName(){return filename;}
    const Column* getPrimaryKey(){return primaryKey;}
    int getRecords(){return records;}
    int getRecordSize(){return recordSize;}
    int getTrueRecordSize(){return trueRecordSize;}
    int getTotalSize(){return records*recordSize;}
    const std::vector<Column*>& getColumns(){return columns;}
    void setRecords(int count){records = count;}
    void increaseRecords(){records++;}

    bool temporary = false;
    bool silent = false;
private:

    std::string name;
    std::vector<Column*> columns;
    std::string filename;
    Column* primaryKey = NULL;

    int records = 0;
    int recordSize = 0;
    int trueRecordSize = 0;
};

enum Condition{
    NONE = 0,
    NORMAL = 1,
    INVALID = 2,
    NO_DATA = 3,
};

}

#endif // TABLE_H
