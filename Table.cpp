#include "Table.h"
#include <fstream>
#include "dbms.h"
#include <cstring>
#include <iomanip>
#include "TableUtil.h"

using namespace std;

namespace dbms {

Column::Column(const std::string &name, DataType type, int size)
    :name(name), type(type), size(size){
    if(type == CHAR)
        trueSize = size + 1;
    else
        trueSize = size;
}

Column::Column(){}

string Column::toString(){
    string str = name;
    if(type == INT)
        str += ":INT";
    else if(type == CHAR)
        str += ":CHAR(" + to_string(size) + ")";
    return str;
}

void Column::fromString(const string &str){
    int idx = str.find(":");
    name = str.substr(0, idx);
    string tmp = str.substr(idx+1);
    if(tmp == "INT"){
        type = INT;
        size = INT_SIZE;
        trueSize = size;
    }
    else if(tmp.substr(0, 4) == "CHAR"){
        type = CHAR;
        idx = tmp.find(")");
        tmp  = tmp.substr(5, idx-5);
        size = stoi(tmp);
        trueSize = size + 1;
    }
}

Column* Column::clone(){
    Column* col = new Column(name, type, size);
    col->tableName = tableName;
    col->offset = offset;
    col->trueSize = trueSize;

    return col;
}

bool Column::compareName(hsql::Expr *expr){
    if(!expr->hasTable() && TableUtil::compareString(name, expr->name))
        return true;
    else if(expr->hasTable() && TableUtil::compareString(tableName, expr->table) && TableUtil::compareString(name, expr->name))
        return true;
    else
        return false;
}

Table::Table()
{

}
Table::~Table(){
    for(auto col : columns){
        delete col;
    }
}

Table::Table(const std::string &name):name(name){
    filename = TABLE_DIR + name + ".tbl";
}

Column* Table::getColumn(const std::string& colName){
    return TableUtil::getColumn(columns, colName);
}

Column* Table::getColumn(   hsql::Expr *expr){
    return TableUtil::getColumn(columns, expr);
}

void Table::setColumns(const vector<Column *> &value, bool changeTableName){
    columns = value;
    recordSize = 0;
    trueRecordSize = 0;
    for(auto ele : columns){
        ele->offset = trueRecordSize;
        if(changeTableName)
            ele->tableName = name;

        recordSize += ele->size;
        trueRecordSize += ele->trueSize;
    }
}

void Table::setPrimaryKey(const string &colName){
    primaryKey = getColumn(colName);
}

bool Table::createFile(){
    ofstream os(filename, ios::out | ios::binary);
    return os.is_open();
}

bool Table::removeFile(){
    return remove(filename.c_str());
}

bool Table::insert(hsql::InsertStatement *stmt){
    // open table file
    fstream os(filename, ios::in|ios::out|ios::binary|ios::app);
    if(!os.is_open()){
        DBMS::log()<<"Can't open database "<<name<<endl;
        return false;
    }

    // insert by values
    if(stmt->type == hsql::InsertStatement::kInsertValues){
        // check size of column and value in statements
        if(stmt->columns != NULL && stmt->columns->size() != stmt->values->size()){
            DBMS::log()<<"Column count doesn't match value count"<<endl;
            return false;
        }
        if(stmt->columns == NULL && columns.size() < stmt->values->size()){
            DBMS::log()<<"INSERT has more expressions than target columns"<<endl;
            return false;
        }

        //  check unknown column in statement
        if(stmt->columns != NULL){
            for(char* col_name : *stmt->columns){
                auto it = getColumn(col_name);
                if(it == NULL){
                    DBMS::log()<<"Unknown column "<<col_name<<" in 'field list'"<<endl;
                    return false;
                }
            }
        }

        // check type consistency and assign expressions
        vector<hsql::Expr*> exprs(columns.size());
        for(int i = 0; i < columns.size(); i++){
            hsql::Expr* expr = NULL;
            Column* col = columns[i];
            // find corresponding value in statement
            if(stmt->columns != NULL){
                for(int j = 0; j < stmt->columns->size(); j++){
                    if(col->name == (*stmt->columns)[j]){
                        expr = (*stmt->values)[j];
                        break;
                    }
                }
            }else{
                if(i < stmt->values->size())
                    expr = (*stmt->values)[i];
            }

            exprs[i]= expr;

            if(expr == NULL)
                continue;

            if(col->type == Column::DataType::CHAR){    // column is CHAR
                if(expr->type == hsql::kExprLiteralString){
                    // check size
                    if(strlen(expr->name) > col->size){
                        DBMS::log()<<"Data too long for column '"<<col->name<<"'"<<endl;
                        return false;
                    }
                }else if(expr->type == hsql::kExprLiteralInt){
                    const char* str = to_string(expr->ival).c_str();
                    // check size
                    if(strlen(str) > col->size){
                        DBMS::log()<<"Data too long for column '"<<col->name<<"'"<<endl;
                        return false;
                    }
                }
            }else{  // column is INT
                // check type consistency
                if(expr->type == hsql::kExprLiteralString){
                    DBMS::log()<<"Invalid integer value: '"<<expr->name<<"' for column '"<<col->name<<"'"<<endl;
                    return false;
                }
            }
        }

        // check primary key violation
        for(int i = 0; i < columns.size(); i++){
            if(columns[i] != primaryKey || exprs[i] == NULL)
                continue;
            // check duplicate
            char* preValue = new char[primaryKey->trueSize];
            for(int j = 0; j < records; j++){
                os.seekg(j*trueRecordSize + primaryKey->offset);
                os.read(preValue, primaryKey->trueSize);

                if(primaryKey->type == Column::INT && *(int*)preValue == exprs[i]->ival){
                    if(!silent)
                        DBMS::log()<<"Duplicate value "<<*(int*)preValue<<" found for primary key "<<primaryKey->name<<endl;
                    return false;
                }
                else if(primaryKey->type == Column::CHAR && strcmp(preValue, exprs[i]->name) == 0){
                    if(!silent)
                        DBMS::log()<<"Duplicate value "<<preValue<<" found for primary key "<<primaryKey->name<<endl;
                    return false;
                }
            }
        }

        // do insert
        char* record = new char[trueRecordSize];
        char* ptrCol = record;
        for(int i = 0; i < columns.size(); i++){
            hsql::Expr* expr = exprs[i];
            Column* col = columns[i];
            if(expr != NULL){
                if(col->type == Column::CHAR){
                    if(expr->type == hsql::kExprLiteralString){
                        memcpy(ptrCol, expr->name, strlen(expr->name)+1);
                    }else{
                        const char* str = to_string(expr->ival).c_str();
                        memcpy(ptrCol, str, strlen(str)+1);
                    }
                }else{
                    memcpy(ptrCol, &expr->ival, INT_SIZE);
                }

            }else{
                memset(ptrCol, 0, col->trueSize);
            }
            ptrCol += col->trueSize;
        }

        os.write(record, trueRecordSize);
        delete record;
        records++;
    }else{  // insert by selection
    }
    os.close();
    return true;
}



bool Table::select(hsql::SelectStatement *stmt, Table* dstTable, hsql::InsertStatement* insertStmt){
    ifstream os(filename, ios::in|ios::binary);

    if(!os.is_open()){
        DBMS::log()<<"Can't open database "<<name<<endl;
        return false;
    }

    // assemble queried columns vector
    vector<pair<string, Column*>> cols;
    if(stmt->selectList->size() == 1 && (*stmt->selectList)[0]->type == hsql::kExprStar){    // select *
        for(auto col : columns)
            cols.push_back(make_pair(col->name, col));
    }else{
        for(hsql::Expr* expr : *stmt->selectList){

            if(expr->type == hsql::kExprLiteralString || expr->type == hsql::kExprColumnRef){   // select C1,C2,...
                string colName = expr->name;
                if(expr->hasTable())
                    colName = string(expr->table) + "." + colName;
                Column* col = getColumn(expr);

                if(col == NULL){
                    DBMS::log()<<"Column '"<<colName<<"' does not exist"<<endl;
                    return false;
                }else{
                    cols.push_back(make_pair(col->name, col));
                }
            }else if(expr->type == hsql::kExprLiteralInt){
                cols.push_back(make_pair(to_string(expr->ival), (Column*)NULL));
            }
        }
    }

    // check where clause
    Condition condition = Condition::NONE;
    if(stmt->whereClause != NULL)
        condition = TableUtil::checkWhere(stmt->whereClause, columns);

    if(condition == Condition::INVALID)
        return false;

    // print header
    if(dstTable == NULL){
        for(auto it : cols){
            Column* col = it.second;
            if(col == NULL || col->type == Column::INT )
                DBMS::log()<<left<<setw(8)<<setfill(' ')<<it.first;
            else if(col->type == Column::CHAR)
                DBMS::log()<<left<<setw(col->size+2 > 8 ? col->size+2 : 8)<<setfill(' ')<<it.first;
        }
        DBMS::log()<<endl;
    }

    if(condition == Condition::NO_DATA)
        return false;

    // assemble condition columns vector
    vector<Column*> conCols;
    if(condition == Condition::NORMAL){
        Column* con = getColumn(stmt->whereClause->expr);
        conCols.push_back(con);
        if(stmt->whereClause->expr2->type == hsql::kExprColumnRef){
            con = getColumn(stmt->whereClause->expr2->name);
            conCols.push_back(con);
        }
    }

    bool hasInsertStatement = insertStmt != NULL;

    // create buffers for checking duplicates
    vector<char*> curValues(cols.size());
    vector<char*> preValues(cols.size());
    auto ptrCurValue = curValues.begin(), ptrPreValue = preValues.begin();
    for(auto it:cols){
        Column* col = it.second;
        if(col == NULL){
            *ptrCurValue = NULL;
            *ptrPreValue = NULL;
        }else{
            *ptrCurValue = new char[col->trueSize];
            *ptrPreValue = new char[col->trueSize];
        }
        ptrCurValue++;
        ptrPreValue++;
    }

    vector<int> validRecordIdx;

    // print records
    for(int i = 0; i < records; i++){
        // check condition
        if(condition == Condition::NORMAL){
            if(!TableUtil::checkColValueWithCondition(os, i*trueRecordSize, conCols, stmt->whereClause))
                continue;
        }

        if(dstTable == NULL){   // print result
            // check duplicate
            bool hasDuplicate = false;
            // set values for current record
            ptrCurValue = curValues.begin();
            for(auto it:cols){
                Column* col = it.second;
                if(col == NULL){
                    ptrCurValue++;
                    continue;
                }

                os.seekg(i*trueRecordSize + col->offset);
                os.read(*ptrCurValue, col->trueSize);
                ptrCurValue++;
            }

            // check previous records
            for(int j : validRecordIdx){
                bool equal = true;
                ptrCurValue = curValues.begin();
                ptrPreValue = preValues.begin();
                for(auto it:cols){
                    Column* col = it.second;
                    if(col == NULL){
                        ptrCurValue++;
                        ptrPreValue++;
                        continue;
                    }

                    os.seekg(j*trueRecordSize + col->offset);
                    os.read(*ptrPreValue, col->trueSize);
                    if(!TableUtil::valueEqual(col, *ptrCurValue, *ptrPreValue)){
                        equal = false;
                        break;
                    }
                    ptrCurValue++;
                    ptrPreValue++;
                }

                if(equal){
                    hasDuplicate = true;
                    break;
                }
            }

            if(hasDuplicate)
                continue;

            validRecordIdx.push_back(i);

            for(auto it:cols){
                Column* col = it.second;
                if(col != NULL){
                    os.seekg(i*trueRecordSize + col->offset);
                    TableUtil::printColValue(os, col);
                }
                else{
                    DBMS::log()<<left<<setw(8)<<setfill(' ')<<it.first;
                }
            }
            DBMS::log()<<endl;
        }else{  // insert into table
            if(!hasInsertStatement){
                insertStmt = new hsql::InsertStatement(hsql::InsertStatement::kInsertValues);
                insertStmt->tableName = strdup(dstTable->getName().c_str());
                insertStmt->columns = new vector<char*>;
            }

            insertStmt->values = new vector<hsql::Expr*>;

            for(auto itFromCol:cols){
                Column* fromCol = itFromCol.second;
                if(fromCol != NULL){
                    if(!hasInsertStatement)
                        insertStmt->columns->push_back(strdup(fromCol->name.c_str()));
                    os.seekg(i*trueRecordSize + fromCol->offset);
                    if(fromCol->type == Column::CHAR){

                        char *bytes = new char[fromCol->trueSize];
                        os.read(bytes, fromCol->trueSize);

                        hsql::Expr* expr = new hsql::Expr(hsql::kExprLiteralString);
                        expr->name = bytes;
                        insertStmt->values->push_back(expr);
                    }else if(fromCol->type == Column::INT){

                        char *bytes = new char[fromCol->trueSize];
                        os.read(bytes, fromCol->trueSize);
                        hsql::Expr* expr = new hsql::Expr(hsql::kExprLiteralInt);\
                        expr->ival = *reinterpret_cast<int*>(bytes);
                        insertStmt->values->push_back(expr);
                        delete bytes;
                    }
                }
                else{
                    hsql::Expr* expr = new hsql::Expr(hsql::kExprLiteralInt);\
                    expr->ival = stoi(itFromCol.first);
                    insertStmt->values->push_back(expr);
                }
            }

            dstTable->silent = true;
            bool insertSuccess = dstTable->insert(insertStmt);
            dstTable->silent = false;
            if(hasInsertStatement){
                delete insertStmt->values;
                insertStmt->values = NULL;
            }
            else
                delete insertStmt;

//            if(!insertSuccess){
//                // delete buffer
//                for(ptrCurValue = curValues.begin(), ptrPreValue = preValues.begin();
//                    ptrCurValue != curValues.end(); ptrCurValue++,ptrPreValue++){
//                    if(*ptrCurValue != NULL)
//                        delete *ptrCurValue;
//                    if(*ptrPreValue != NULL)
//                        delete *ptrPreValue;
//                }
//                return false;
//            }
        }
    }

    // delete buffer
    for(ptrCurValue = curValues.begin(), ptrPreValue = preValues.begin();
        ptrCurValue != curValues.end(); ptrCurValue++,ptrPreValue++){
        if(*ptrCurValue != NULL)
            delete *ptrCurValue;
        if(*ptrPreValue != NULL)
            delete *ptrPreValue;
    }
    return true;
}

bool Table::join(Column *leftCol, Table *rightTable, Column *rightCol, Table *dstTable){
    ifstream leftFile(filename, ios::in|ios::binary);
    if(!leftFile.is_open()){
        DBMS::log()<<"Can't open database "<<name<<endl;
        return false;
    }

    ifstream rightFile(rightTable->getFileName(), ios::in|ios::binary);
    if(!rightFile.is_open()){
        DBMS::log()<<"Can't open database "<<rightTable->getName()<<endl;
        return false;
    }

    ofstream dstFile(dstTable->getFileName(), ios::out|ios::binary|ios::app);
    if(!dstFile.is_open()){
        DBMS::log()<<"Can't open database "<<dstTable->getName()<<endl;
        return false;
    }

    // loop left file on column
    char* leftBuffer = new char[leftCol->trueSize];
    char* rightBuffer = new char[rightCol->trueSize];
    char* leftRecordBuffer = new char[trueRecordSize];
    char* rightRecordBuffer = new char[rightTable->getTrueRecordSize()];
    for(int leftIdx = 0; leftIdx < records; leftIdx++){
        leftFile.seekg(leftIdx * trueRecordSize);
        leftFile.read(leftRecordBuffer, trueRecordSize);

        leftFile.seekg(leftIdx * trueRecordSize + leftCol->offset);
        leftFile.read(leftBuffer, leftCol->trueSize);

        for(int rightIdx = 0; rightIdx < rightTable->getRecords(); rightIdx++){
            rightFile.seekg(rightIdx * rightTable->getTrueRecordSize() + rightCol->offset);
            rightFile.read(rightBuffer, rightCol->trueSize);

            // compare value
            if(leftCol->type == Column::INT && *(int*)leftBuffer == *(int*)rightBuffer ||
                    leftCol->type == Column::CHAR && strcmp(leftBuffer, rightBuffer) == 0){
                rightFile.seekg(rightIdx * rightTable->getTrueRecordSize());
                rightFile.read(rightRecordBuffer, rightTable->getTrueRecordSize());

                dstFile.write(leftRecordBuffer, trueRecordSize);
                dstFile.write(rightRecordBuffer, rightTable->getTrueRecordSize());

                dstTable->increaseRecords();
            }
        }
    }

    delete leftBuffer, rightBuffer, leftRecordBuffer, rightRecordBuffer;
    return true;
}
}
