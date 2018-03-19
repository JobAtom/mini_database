#ifndef TABLEUTIL_H
#define TABLEUTIL_H

#include <fstream>
#include "Table.h"
#include "sql/statements.h"

namespace dbms{
class TableUtil
{
public:
    TableUtil();

    static char* readColBytes(std::ifstream& os, Column* col);
    static void printColValue(std::ifstream& os, Column* col);

    static Condition checkWhere(hsql::Expr* whereClause, const std::vector<Column*>& columns);
    static bool isColumnExist(const hsql::Expr* expr, const std::vector<Column*>& columns);
    static bool checkColValueWithCondition(std::ifstream& os, int offset, std::vector<Column*> cols, hsql::Expr* whereClause);

    static Column* getColumn(const std::vector<Column*>& columns, const std::string& colName);
    static Column* getColumn(const std::vector<Column*>& columns, hsql::Expr* expr);

    static bool valueEqual(Column* col, char* val1, char* val2);

    static bool compareString(const std::string& str1, const std::string& str2);
    static bool compareString(const std::string& str1, const char* str2);
    static bool compareString(const char* str1, const char* str2);
};
}
#endif // TABLEUTIL_H
