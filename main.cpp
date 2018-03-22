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
void loadFromFile(map<string, table*> map_list);
void saveToFile(map<string, table*> map_list);

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
                map<string, table*> table_list;
                loadFromFile(table_list);
                executeStatement(result->getMutableStatement(i));
                saveToFile(table_list);

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

    table* newtable = new table(stmt->tableName);


}


void loadFromFile(map<string, table*> map_list){

    ifstream is("CATALOG.txt");
    string line;
    while(getline(is, line)){
        // tablename
        string name = line.substr(10);
        table* temp_table = new table(name);

        cout << name <<endl;
        map_list.insert(make_pair(name, temp_table));
    }

    is.close();

}
void saveToFile(map<string, table*> map_list){
    if(map_list.size() == 0){
        return;
    }
    ofstream os("CATALOG.txt");
    os.close();
}