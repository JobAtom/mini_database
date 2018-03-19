#include "dbms.h"
#include <iostream>
#include "SQLParser.h"
#include "sqlhelper.h"

namespace dbms{

ostream* DBMS::os;

DBMS::DBMS() : mode(RUN_MODE::INTERACTIVE)
{
}

DBMS::~DBMS(){
    delete schema;

    if(dynamic_cast<ofstream*>(os))
        delete os;
}

void DBMS::setMode(RUN_MODE mode){
    this->mode = mode;

    if(mode == RUN_MODE::SCRIPT){
        os = new ofstream("output.txt");
//        os = &std::cout;
    }else{
        os = &std::cout;
    }

    schema = new Schema();
}

bool DBMS::processQuery(const string &query){
    // parse a given query
    hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);

    // check whether the parsing was successful
    if (result->isValid()) {

        for (unsigned i = 0; i < result->size(); ++i) {
            if(result->getStatement(i)->type() == hsql::StatementType::kStmtQuit){
                delete result;
                return true;
            }
            else{
                schema->executeStatement(result->getMutableStatement(i));
                log()<<endl;
            }

            // Print a statement summary.
//            hsql::printStatementInfo(result->getStatement(i));
        }
        delete result;
        return false;
    } else {
        log()<<"Given string is not a valid SQL query."<<endl
            <<result->errorMsg()<<"("<<result->errorLine()<<":"<<result->errorColumn()<<")"<<endl;
        delete result;
        return false;
    }

}
}
