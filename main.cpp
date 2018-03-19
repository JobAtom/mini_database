#include <iostream>
#include <stdlib.h>
#include <string>

// include the sql parser
#include "SQLParser.h"

// contains printing utilities
#include "sqlhelper.h"
#include <cassert>
#include <vector>
#include <fstream>
#include <sstream>
#include "dbms.h"

using namespace std;
using namespace dbms;

int main(int argc, char *argv[])
{
    if (argc <= 1) {
        fprintf(stderr, "Usage:\n./dbms script=filename\n\tOR\n./dbms \"SQL query\"\n");
        return -1;
    }

    std::string param = argv[1];
    DBMS db;
    if(param.find("script=") == 0){
        db.setMode(RUN_MODE::SCRIPT);

        std::ifstream file(param.substr(7));
        std::stringstream buffer;
        buffer << file.rdbuf();
        db.processQuery(buffer.str());
    }else{

        string query = param;

        db.setMode(RUN_MODE::INTERACTIVE);

        while(true){
            if(db.processQuery(query))
                return 0;

            cout<<"SQL > ";
            query.clear();
            do{
                string line;
                getline(cin, line);
                query += " " + line;
            }while (query.find(";") == string::npos);

        }
    }
}
