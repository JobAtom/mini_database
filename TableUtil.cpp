#include "TableUtil.h"
#include <iomanip>
#include "dbms.h"
#include <cstring>
#include <locale>

using namespace std;

namespace dbms {

TableUtil::TableUtil()
{

}

char* TableUtil::readColBytes(ifstream& os, Column* col){
    char* bytes = new char[col->trueSize];
    os.read(bytes, col->trueSize);
    return bytes;
}

void TableUtil::printColValue(ifstream& os, Column *col){
    char* bytes = readColBytes(os, col);
    if(col->type == Column::INT)
        DBMS::log()<<left<<setw(8)<<setfill(' ')<<*(int*)bytes;
    else{
        DBMS::log()<<left<<setw(col->size+2 > 8 ? col->size+2 : 8)<<setfill(' ')<<bytes;
    }
    delete bytes;
}

Condition TableUtil::checkWhere(hsql::Expr *whereClause, const vector<Column*>& columns){

    if(whereClause->type != hsql::kExprOperator ||
            whereClause->opType != hsql::Expr::SIMPLE_OP ||
            whereClause->opChar != '='){
        DBMS::log()<<"Invalide where clause"<<endl;
        return Condition::INVALID;
    }

    if(whereClause->expr->type == hsql::kExprLiteralString){    // left is string
        if(whereClause->expr2->type == hsql::kExprLiteralInt){  // right is int
            DBMS::log()<<"Invalid integer value: '"<<whereClause->expr->name<<"' for column '"<<whereClause->expr2->ival<<"'"<<endl;
            return Condition::INVALID;
        }else if(whereClause->expr2->type == hsql::kExprLiteralString){ // right is string
            if(strcmp(whereClause->expr->name, whereClause->expr2->name) == 0)
                return Condition::NONE;
            else
                return Condition::NO_DATA;
        }else if(whereClause->expr2->type == hsql::kExprColumnRef){ // right is column
            hsql::Expr* tmp = whereClause->expr;
            whereClause->expr = whereClause->expr2;
            whereClause->expr2 = tmp;
        }else{
            DBMS::log()<<"Invalide where clause"<<endl;
            return Condition::INVALID;
        }
    }else if(whereClause->expr->type == hsql::kExprLiteralInt){    // left is int
        if(whereClause->expr2->type == hsql::kExprLiteralInt){  // right is int
            if(whereClause->expr->ival == whereClause->expr2->ival)
                return Condition::NONE;
            else
                return Condition::NO_DATA;
        }else if(whereClause->expr2->type == hsql::kExprLiteralString){ // right is string
            DBMS::log()<<"Invalid integer value: '"<<whereClause->expr2->name<<"' for column '"<<whereClause->expr->ival<<"'"<<endl;
            return Condition::INVALID;
        }else if(whereClause->expr2->type == hsql::kExprColumnRef){ // right is column
            hsql::Expr* tmp = whereClause->expr;
            whereClause->expr = whereClause->expr2;
            whereClause->expr2 = tmp;
        }else{
            DBMS::log()<<"Invalide where clause"<<endl;
            return Condition::INVALID;
        }
    }else if(whereClause->expr->type == hsql::kExprColumnRef){    // left is column

    }else{
        DBMS::log()<<"Invalide where clause"<<endl;
        return Condition::INVALID;
    }

    if(isColumnExist(whereClause, columns))
        return Condition::NORMAL;
    else
        return Condition::INVALID;
}



bool TableUtil::isColumnExist(const hsql::Expr *expr, const std::vector<Column *> &columns){

    auto it = getColumn(columns, expr->expr->name);
    if(it == NULL){
        DBMS::log()<<"Columns '"<<expr->expr->name<<"' does not exist"<<endl;
        return false;
    }

    if(it->type == Column::CHAR && expr->expr2->type == hsql::kExprLiteralString ||
            it->type == Column::INT && expr->expr2->type == hsql::kExprLiteralInt){
            return true;
    }else if(expr->expr2->type == hsql::kExprColumnRef){
        auto it2 = getColumn(columns, expr->expr2->name);
        if(it2 == NULL){
            DBMS::log()<<"Columns '"<<expr->expr2->name<<"' does not exist"<<endl;
            return false;
        }
        if(it->type != it2->type){
            DBMS::log()<<"Data type mismatch"<<endl;
            return false;
        }
        return true;
    }else{
        DBMS::log()<<"Data type mismatch"<<endl;
        return false;
    }

}

bool TableUtil::checkColValueWithCondition(ifstream& os,  int offset, vector<Column *> cols, hsql::Expr* whereClause){
    bool equal = false;

    Column* col = cols[0];
    os.seekg(offset + col->offset);
    char* bytes = readColBytes(os, col);
    if(cols.size() > 1){
        os.seekg(offset + cols[1]->offset);
        char* bytes2 = readColBytes(os, cols[1]);
        equal = strcmp(bytes, bytes2) == 0;

        delete bytes2;
    }else{
        if(col->type == Column::INT){
            if(*(int*)bytes == whereClause->expr2->ival)
                equal = true;
        }
        else if(col->type == Column::CHAR){
            equal = strcmp(bytes, whereClause->expr2->name) == 0;
        }
    }
    delete bytes;
    return equal;
}

Column* TableUtil::getColumn(const std::vector<Column*>& columns, const std::string& colName){
    for(Column* col : columns){
        if(compareString(colName, col->name))
            return col;
    }
    return NULL;
}

Column* TableUtil::getColumn(const std::vector<Column*>& columns,   hsql::Expr* expr){
    for(Column* col : columns){
        if(col->compareName(expr))
            return col;
    }
    return NULL;
}

bool TableUtil::valueEqual(Column *col, char *val1, char *val2){
    if(col->type == Column::INT){
        return *(int*)val1 == *(int*)val2;
    }else{
        return strcmp(val1, val2) == 0;
    }
}

bool TableUtil::compareString(const string &str1, const string &str2){
    if(str1.size() != str2.size())
        return false;
    for(int i = 0; i < str1.size(); i++)
        if(tolower(str1[i]) != tolower(str2[i]))
            return false;
    return true;
}
bool TableUtil::compareString(const string &str1, const char* str2){
    if(str1.size() != strlen(str2))
        return false;
    for(int i = 0; i < str1.size(); i++)
        if(tolower(str1[i]) != tolower(str2[i]))
            return false;
    return true;
}
bool TableUtil::compareString(const char* str1, const char* str2){
    if(strlen(str1) != strlen(str2))
        return false;
    for(int i = 0; i < strlen(str1); i++)
        if(tolower(str1[i]) != tolower(str2[i]))
            return false;
    return true;
}
}
