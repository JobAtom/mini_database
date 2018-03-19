#include "Schema.h"
#include "dbms.h"
#include <sstream>
#include <iterator>
#include <cstring>
#include "TableUtil.h"
using namespace std;

namespace dbms{
Schema::Schema()
{
    loadFromFile();
}

Schema::~Schema(){
    for(auto it = tables.cbegin(); it != tables.cend();){
        if(it->second->temporary){
            it->second->removeFile();
            delete it->second;
            it = tables.erase(it);
        }else{
            it++;
        }
    }

    saveToFile();

    for(auto ele : tables)
        delete ele.second;
}

// split a string with delimiter
inline std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }

    return elems;
}

void Schema::loadFromFile(){
    ifstream is(CATALOG_FILE);
    string line;
    while(getline(is, line)){
        // tablename
        string name = line.substr(10);
        Table* tbl = new Table(name);
        // columns
        getline(is, line);
        vector<Column*> columns;
        string strCol = line.substr(8);
        vector<string> tokens = split(strCol, ',');
        for(auto col_str:tokens){

            Column* col = new Column();
            col->fromString(col_str);
            columns.push_back(col);
        }

        tbl->setColumns(columns);

        // primary key
        getline(is, line);
        string key = line.substr(12);
        if(key != "NULL"){
            tbl->setPrimaryKey(key);
        }

        // record size
        getline(is, line);
        // total size
        getline(is, line);
        // records
        getline(is, line);
        string records = line.substr(8);
        tbl->setRecords(stoi(records));

        tables.insert(make_pair(name, tbl));
    }

    is.close();
}

void Schema::saveToFile(){
    ofstream os(CATALOG_FILE);

    for(auto t : tables){
        Table* tbl = t.second;
        os<<"tablename="<<tbl->getName()<<endl;
        os<<"columns=";
        string str;
        for(auto it : tbl->getColumns()){
           str += it->toString() + ",";
        }
        os<<str.substr(0, str.size()-1)<<endl;
        os<<"primary key=";
        if(tbl->getPrimaryKey() != NULL)
            os<<tbl->getPrimaryKey()->name<<endl;
        else
            os<<"NULL"<<endl;
        os<<"recordsize="<<tbl->getRecordSize()<<endl;
        os<<"totalsize="<<tbl->getTotalSize()<<endl;
        os<<"records="<<tbl->getRecords()<<endl;
    }

    os.close();
}

Table* Schema::getTable(const string &tableName){
    for(auto it : tables){
        if(TableUtil::compareString(tableName, it.first))
            return it.second;
    }
    return NULL;
}

void Schema::executeStatement(hsql::SQLStatement *stmt){
    switch (stmt->type()) {
    case hsql::kStmtCreate:
        executeCreate((hsql::CreateStatement*)stmt);
        break;
    case hsql::kStmtSelect:
        executeSelect((hsql::SelectStatement*)stmt);
        break;
    case hsql::kStmtInsert:
        executeInsert((hsql::InsertStatement*)stmt);
        break;
    case hsql::kStmtShow:
        executeShow((hsql::ShowStatement*)stmt);
        break;
    case hsql::kStmtDrop:
        executeDrop((hsql::DropStatement*)stmt);
        break;
    default:
        break;
    }
}

void Schema::executeCreate(hsql::CreateStatement *stmt){
    Table* table = getTable(stmt->tableName);
    if(table != NULL){
        DBMS::log()<<"Table "<<stmt->tableName<<" already exists"<<endl;
        return;
    }

    Table* tbl = new Table(stmt->tableName);
    if(tbl->createFile()){
        vector<Column*> columns;
        for (hsql::ColumnDefinition* col_def : *stmt->columns) {
            Column::DataType type;
            int size;

            if(col_def->type == hsql::ColumnDefinition::DataType::INT){
                type = Column::INT;
                size = INT_SIZE;
            }
            else if(col_def->type == hsql::ColumnDefinition::DataType::TEXT){
                type = Column::CHAR;
                size = col_def->size;
            }else{
                DBMS::log()<<"Invalid column type."<<endl;
                return;
            }
            Column* col = new Column(col_def->name, type, size);
            columns.push_back(col);
        }

        tbl->setColumns(columns);
        if(stmt->primaryKey != NULL)
            tbl->setPrimaryKey(stmt->primaryKey->name);

        tables.insert(pair<string, Table*>(stmt->tableName, tbl));

        DBMS::log()<<"Create table "<<stmt->tableName<<" success"<<endl;
    }
    else{
        delete tbl;
        DBMS::log()<<"Create table "<<stmt->tableName<<" fail"<<endl;
    }
}

void Schema::executeInsert(hsql::InsertStatement *stmt){
    Table* toTable = getTable(stmt->tableName);
    if(toTable == NULL){
        DBMS::log()<<"Table "<<stmt->tableName<<" does not exist"<<endl;
    }else{
        if(stmt->type == hsql::InsertStatement::kInsertValues){ // insert from values
            if(toTable->insert(stmt))
                DBMS::log()<<"Insert success"<<endl;
        }else{
            if(stmt->select->fromTable->type == hsql::kTableName){  // insert from signle select

            }else if(stmt->select->fromTable->type == hsql::kTableSelect ||
                     stmt->select->fromTable->type == hsql::kTableJoin){  // insert from nested select
                if(!createRefTable(stmt->select->fromTable))
                    return;
            }

            string fromTableName = stmt->select->fromTable->getName();
            Table* fromTbl = getTable(fromTableName);
            if(fromTbl == NULL){
                DBMS::log()<<"Table "<<fromTableName<<" does not exist"<<endl;
                return;
            }

            // compare column count
            int numFromCols, numToCols;
            if(stmt->select->selectList->size() == 1 && (*stmt->select->selectList)[0]->type == hsql::kExprStar){
                numFromCols = fromTbl->getColumns().size();
            }else{
                numFromCols = stmt->select->selectList->size();
            }

            if(stmt->columns == NULL){
                numToCols = toTable->getColumns().size();
                if(numFromCols > numToCols){
                    DBMS::log()<<"INSERT has more expressions than target columns"<<endl;
                    return;
                }
            }else{
                numToCols = stmt->columns->size();
                if(numToCols > numFromCols){
                    DBMS::log()<<"INSERT has more target columns than expressions"<<endl;
                    return;
                }else if(numToCols < numFromCols){
                    DBMS::log()<<"INSERT has more expressions than target columns"<<endl;
                    return;
                }

            }

            // insert from selection
            stmt->type = hsql::InsertStatement::kInsertValues;
            bool success = fromTbl->select(stmt->select, toTable, stmt);
            stmt->type = hsql::InsertStatement::kInsertSelect;
            if(success)
                DBMS::log()<<"Insert success"<<endl;
        }
    }
}

void Schema::executeSelect(hsql::SelectStatement *stmt){
    if(stmt->fromTable->type == hsql::kTableName){
        Table* table = getTable(stmt->fromTable->name);
        if(table == NULL){
            DBMS::log()<<"Table "<<stmt->fromTable->name<<" does not exist"<<endl;
        }else{
            table->select(stmt);
        }
    }else{
        if(createRefTable(stmt->fromTable)){
            Table* fromTbl = getTable(stmt->fromTable->getName());
            if(fromTbl != NULL)
                fromTbl->select(stmt);
        }
    }
}

bool Schema::createRefTable(hsql::TableRef *ref){
    if(ref->type == hsql::kTableSelect){
        return createRefTableFromSelect(ref);
    }else if(ref->type == hsql::kTableJoin){
        return createRefTableFromJoin(ref);
    }
    return true;
}

bool Schema::createRefTableFromSelect(hsql::TableRef *ref){
    if(ref->alias == NULL){
        DBMS::log()<<"Subquery in FROM must have an alias"<<endl;
        return false;
    }

    string tmpTableName = ref->alias;

    if(ref->select->fromTable->type != hsql::kTableName){
        if(!createRefTable(ref->select->fromTable))
            return false;
    }else{
    }

    string fromTableName = ref->select->fromTable->getName();

    Table* table = getTable(tmpTableName);
    if(table != NULL){
        DBMS::log()<<"Table "<<tmpTableName<<" already exists"<<endl;
        return false;
    }

    Table* fromTbl = getTable(fromTableName);
    if(fromTbl == NULL){
        DBMS::log()<<"Table "<<fromTableName<<" does not exist"<<endl;
        return false;
    }

    Table* tmpTbl = new Table(tmpTableName);
    tmpTbl->temporary = true;
    if(tmpTbl->createFile()){
        vector<Column*> columns;
        if(ref->select->selectList->size() == 1 && (*ref->select->selectList)[0]->type == hsql::kExprStar){    // select *
            for(auto col : fromTbl->getColumns()){
                columns.push_back(col->clone());
            }
        }else{
            for (hsql::Expr* expr : *(ref->select->selectList)) {
                Column* col = fromTbl->getColumn(expr->name);
                if(col == NULL){
                    DBMS::log()<<"Column '"<<expr->name<<"' does not exist"<<endl;
                    delete tmpTbl;
                    return false;
                }

                columns.push_back(col->clone());
            }
        }

        tmpTbl->setColumns(columns);


        if(fromTbl->select(ref->select, tmpTbl))
            tables.insert(pair<string, Table*>(tmpTableName, tmpTbl));
        else{
            delete tmpTbl;
            return false;
        }
    }
    else{
        delete tmpTbl;
        return false;
    }

    return true;
}

bool Schema::createRefTableFromJoin(hsql::TableRef *ref){
    string leftTableName = ref->join->left->getName();
    if(!createRefTable(ref->join->left))
        return false;
    Table* leftTable = getTable(leftTableName);
    if(leftTable == NULL){
        DBMS::log()<<"Table "<<leftTableName<<" does not exist"<<endl;
        return false;
    }

    string rightTableName = ref->join->right->getName();
    if(!createRefTable(ref->join->right))
        return false;
    Table* rightTable = getTable(rightTableName);
    if(rightTable == NULL){
        DBMS::log()<<"Table "<<rightTableName<<" does not exist"<<endl;
        return false;
    }

    if(ref->join->condition->type != hsql::kExprOperator ||
            ref->join->condition->opType != hsql::Expr::SIMPLE_OP ||
            ref->join->condition->opChar != '='){
        DBMS::log()<<"Invalide join clause"<<endl;
        return false;
    }

    string leftColName = ref->join->condition->expr->name;
    if(ref->join->condition->expr->hasTable())
        leftColName = string(ref->join->condition->expr->table) + "." + leftColName;
    Column* leftCol = leftTable->getColumn(ref->join->condition->expr);
    if(leftCol == NULL)
        leftCol = leftTable->getColumn(ref->join->condition->expr2);
    if(leftCol == NULL){
        DBMS::log()<<"Column '"<<leftColName<<"' does not exist"<<endl;
        return false;
    }

    string rightColName = ref->join->condition->expr2->name;
    if(ref->join->condition->expr2->hasTable())
        rightColName = string(ref->join->condition->expr2->table) + "." + rightColName;
    Column* rightCol = rightTable->getColumn(ref->join->condition->expr2);
    if(rightCol == NULL)
        rightCol = rightTable->getColumn(ref->join->condition->expr);
    if(rightCol == NULL){
        DBMS::log()<<"Column '"<<rightColName<<"' does not exist"<<endl;
        return false;
    }

    if(leftCol->type != rightCol->type){
        DBMS::log()<<"Data type of '"<<leftColName<<" and "<<rightColName<<"' do not match"<<endl;
        return false;
    }

    string tmpTableName = "tmp" + to_string(tables.size());
    Table* tmpTbl = new Table(tmpTableName);
    tmpTbl->temporary = true;
    if(tmpTbl->createFile()){
        vector<Column*> columns;
        for(auto it : leftTable->getColumns()){
            columns.push_back(it->clone());
        }
        for(auto it : rightTable->getColumns()){
            columns.push_back(it->clone());
        }

        tmpTbl->setColumns(columns, false);

        if(leftTable->join(leftCol, rightTable, rightCol, tmpTbl))
            tables.insert(make_pair(tmpTableName, tmpTbl));
        else{
            delete tmpTbl;
            return false;
        }
    }
    else{
        delete tmpTbl;
        return false;
    }

    ref->alias = strdup(tmpTableName.c_str());
    return true;
}

void Schema::executeShow(hsql::ShowStatement *stmt){
    vector<string> tableNames;
    if(stmt->type == hsql::ShowStatement::kTable){
        tableNames.push_back(stmt->tableName);
    }else{
        for(auto it : tables)
            tableNames.push_back(it.first);
    }

    for(string tableName : tableNames){
        Table* tbl = getTable(tableName);
        if(tbl == NULL){
            DBMS::log()<<"Table "<<tableName<<" does not exist"<<endl;
            return;
        }
        DBMS::log()<<"Table "<<tbl->getName()<<"(";
        string str;
        for(auto it : tbl->getColumns()){
           str += it->toString() + ",";
        }
        DBMS::log()<<str<<"PRIMARY KEY=";
        if(tbl->getPrimaryKey() != NULL)
            DBMS::log()<<tbl->getPrimaryKey()->name;
        else
            DBMS::log()<<"NULL";
        DBMS::log()<<")"<<endl;
    }
}

void Schema::executeDrop(hsql::DropStatement *stmt){
    string tableName = stmt->name;
    for(auto it = tables.cbegin(); it != tables.cend();it++){
        if(TableUtil::compareString(tableName, it->first)){
            it->second->removeFile();
            delete it->second;
            tables.erase(it);
            DBMS::log()<<"Table "<<tableName<<" is removed"<<endl;
            return;
        }
    }
    DBMS::log()<<"Table "<<tableName<<" does not exist"<<endl;
}

}
