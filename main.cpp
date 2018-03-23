//
// Created by JinAobo on 3/20/18.
//

#include <iostream>
#include <string>
#include "SQLParser.h"
#include "sqlhelper.h"
#include <sstream>
#include <iterator>
#include <cstring>
#include <map>
#include <fstream>
#include "table.h"
#include "util.h"

using namespace std;

void executeStatement(hsql::SQLStatement *stmt, map<string, table*> &table_list);
void createTable(hsql::CreateStatement *stmt, map<string, table*> &table_list);
void insertTable(hsql::InsertStatement *stmt, map<string, table*> &table_list);
void loadFromFile(map<string, table*> &map_list);
void saveToFile(map<string, table*> &map_list);

int main(int argc, char * argv[]){
    if(argc <= 1){
        cout<<"please input query to start SQL!"<<endl;
        exit(1);
    }
    string query = "";
    for(int i = 1 ; i < argc; i ++ ){
        query += argv[i];
        query += " ";
    }
    query += ";";

    if(query.find("script="))
    {
        //read script and run sql by script

    }
    map<string, table*> table_list;
    //loadFromFile(table_list);
    while(true) {
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);
        // check whether the parsing was successful
        if (result->isValid()) {
            for (unsigned i = 0; i < result->size(); ++i) {
                //run sql query
                executeStatement(result->getMutableStatement(i), table_list);
                saveToFile(table_list);

            }
        } else {
            cout << "Given string is not a valid SQL query." << endl
                 << result->errorMsg() << "(" << result->errorLine() << ":" << result->errorColumn() << ")" << endl;
        }
        query.clear();
        cout << "SQL>";
        while(query.find(";") == string::npos){
            string line;
            getline(cin, line);
            query += line;
        }
        if (query == "quit;") {
            saveToFile(table_list);
            exit(0);
        }
    }
    return 0;
}

void executeStatement(hsql::SQLStatement *stmt, map<string, table*> &table_list){
    switch (stmt->type()) {
        case hsql::kStmtCreate:
            cout << "Create" <<endl;
            createTable((hsql::CreateStatement*)stmt, table_list);
            break;
        case hsql::kStmtSelect:
            cout << "Select" <<endl;
            //executeSelect((hsql::SelectStatement*)stmt);
            break;
        case hsql::kStmtInsert:
            cout << "Insert" <<endl;
            insertTable((hsql::InsertStatement*)stmt, table_list);
            break;
        case hsql::kStmtShow:
            cout << "Show" <<endl;
            //executeShow((hsql::ShowStatement*)stmt);
            break;
        case hsql::kStmtDrop:
            cout << "Drop" <<endl;
            //executeDrop((hsql::DropStatement*)stmt, table_list);
            break;
        default:
            break;
    }
}


void createTable(hsql::CreateStatement *stmt, map<string, table*> &table_list){
    cout << "Creating table " << stmt->tableName << "... "<<endl;

    table* newtable = new table(stmt->tableName);
    vector<column* > cols;
    //put cols to table
    for(hsql::ColumnDefinition* col_def : *stmt->columns){
        string flag ;
        int size = 0;
        if (col_def-> type == hsql::ColumnDefinition::DataType::INT){
            flag = "INT";
            size = 8;
        }
        else if (col_def->type == hsql::ColumnDefinition::DataType::TEXT){
            flag = "CHAR";
            size = col_def->size;
        }
        column* newcol = new column(col_def->name, flag, size);
        cols.push_back(newcol);
    }
    newtable->addColumn(cols);
    ofstream os(newtable->getName(), ios::out | ios::binary);

    table_list.insert(make_pair(stmt->tableName, newtable));
    //set primary key
    if(stmt->primaryKey != NULL){
        newtable->setPrimaryKey(stmt->primaryKey->name);
    }
    os.close();

}
void insertTable(hsql::InsertStatement *stmt, map<string, table*> &table_list){
    cout<<"Insert into table"<<endl;
    table* totable = util::getTable(stmt->tableName, table_list);
    if(totable == NULL){
        cout<<"table "<<stmt->tableName<<" not exits"<<endl;
        return;
    }
    if (stmt->type == hsql::InsertStatement::kInsertValues){
        if(totable->insert(stmt)){
            cout << "insert successful" << endl;
        }
    }


}

//need more founctions
void loadFromFile(map<string, table*> &map_list){

    ifstream is("CATALOG.txt");
    string line;
    while(getline(is, line)){
        // tablename
        string name = line.substr(10);
        table* temp_table = new table(name);

        //cout << name <<endl;
        map_list.insert(make_pair(name, temp_table));
    }

    is.close();

}
void saveToFile(map<string, table*> &map_list){
    if(map_list.size() == 0){
        return;
    }
    ofstream os("CATALOG.txt");
    for(auto tl: map_list){
        os << "tablename=";
        os << tl.first <<endl;
        os << "columns=";
        string  temp_str = "";
        for(auto col : tl.second->table_cols){
            temp_str += col->name + ": " + col->flag + ", ";
        }
        os << temp_str.substr(0, temp_str.size() - 2 ) << endl;
        if(tl.second->getPrimaryKey()!= NULL)
            os << "primaryKey=" << tl.second->getPrimaryKey()->name << endl;
        else
            os << "primaryKey=NULL" << endl;
        os << "recordsize=" << endl;
        os << "totalrecordsize=" << endl;
        os << "records="<< endl;
        os << "\n" << endl;

    }

    os.close();
}