//
// Created by JinAobo on 3/20/18.
//

#include <iostream>
#include <string>
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;

int main(int argc, char * argv[]){
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

    while(true) {
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);

        // check whether the parsing was successful
        if (result->isValid()) {
            for (unsigned i = 0; i < result->size(); ++i) {
                //run sql query
                cout<< 'run sql query'<<endl;
                
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