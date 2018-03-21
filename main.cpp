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

using namespace std;

void executeStatement(hsql::SQLStatement *stmt);
void createTable(hsql::CreateStatement *stmt);
void loadFromFile();

int main(int argc, char * argv[]){
    loadFromFile();
    cout<<"begin mini_database project!!!"<<endl;
    if(argc <= 1){
        cout<<"please input query to start SQL!"<<endl;
        exit(1);
    }
    string query = "";
    for(int i = 1 ; i < argc; i ++ ){
        query += argv[i];
        query += " ";
    }

    if(query.find("script="))
    {
        //read script and run sql by script

    }

    while(true) {
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);

        // check whether the parsing was successful
        if (result->isValid()) {
            for (unsigned i = 0; i < result->size(); ++i) {
                //run sql query
                loadFromFile();
                cout<< 'run sql query'<<endl;
                executeStatement(result->getMutableStatement(i));

            }
        } else {
            cout << "Given string is not a valid SQL query." << endl
                 << result->errorMsg() << "(" << result->errorLine() << ":" << result->errorColumn() << ")" << endl;
        }
        delete result;
        query.clear();
        cout << "SQL>";
        while(query.find(";") == string::npos){
            string line;
            getline(cin, line);
            query += line;
        }
        if (query == "quit;")
            return 0;
    }
    return 0;
}

void executeStatement(hsql::SQLStatement *stmt){
    switch (stmt->type()) {
        case hsql::kStmtCreate:
            cout << "Create" <<endl;
            createTable((hsql::CreateStatement*)stmt);
            break;
        case hsql::kStmtSelect:
            cout << "Select" <<endl;
            //executeSelect((hsql::SelectStatement*)stmt);
            break;
        case hsql::kStmtInsert:
            cout << "Insert" <<endl;
            //executeInsert((hsql::InsertStatement*)stmt);
            break;
        case hsql::kStmtShow:
            cout << "Show" <<endl;
            //executeShow((hsql::ShowStatement*)stmt);
            break;
        case hsql::kStmtDrop:
            cout << "Drop" <<endl;
            //executeDrop((hsql::DropStatement*)stmt);
            break;
        default:
            break;
    }
}

void createTable(hsql::CreateStatement *stmt){
    cout << "Creating table " << stmt->tableName << "... "<<endl;

    //Table* table = getTable(stmt->tableName);
    //cout << table <<endl;
    // manipulate the table
}

void loadFromFile(){

    ifstream is("CATALOG.txt");
    string line;
    cout << "catalog" <<endl;
    while(getline(is, line)){
        // tablename
        string name = line.substr(10);
        cout << name <<endl;
    }

    is.close();
}