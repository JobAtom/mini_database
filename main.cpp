//
// Created by JinAobo on 3/20/18.
//

#include <iostream>
#include <stdlib.h>
#include <string>
#include "SQLParser.h"
#include "sqlhelper.h"
#include <sstream>
#include <iterator>
#include <cstring>
#include <map>
#include <vector>
#include <fstream>
#include "table.h"
#include "util.h"
#include <pthread.h>
#include "util.h"
#include "ThreadPool.cpp"
#include <algorithm>
#include <mutex>





using namespace std;

void executeStatement(hsql::SQLStatement *stmt, map<string, table*> &table_list);
void createTable(hsql::CreateStatement *stmt, map<string, table*> &table_list);
bool insertTable(hsql::InsertStatement *stmt, map<string, table*> &table_list, bool check);
void executeShow(hsql::ShowStatement *stmt, map<string, table*> &table_list);
void executeSelect(hsql::SelectStatement *stmt,  map<string, table*> &table_list);
bool executeUpdate(hsql::UpdateStatement *stmt, map<string, table*> &table_list, bool check);
void joinTable(table* t1, table* t2, hsql::SelectStatement *stmt);
void executeTransaction(string transbuffer, vector<string> lockitem);

void loadFromFile(map<string, table*> &map_list);
void saveToFile(map<string, table*> &map_list);
void printTableList(map<string, table*> table_list);
void loadTableList(map<string, table*> & table_list);
inline std::vector<std::string> split(const std::string &s, char delim);
void Lowercase(string &s);
bool check_true_size(vector<string> lockitem);
void unlock_items(vector<string> lockitem, int x);
string removespace(string s);


map<string, int> locks;
map<string, table*> table_list;

queue <int> checkthread;
mutex con;
string output_file = "output.txt";

int main(int argc, char * argv[]){

    if(argc <= 1){
        cout<<"please input query to start or use script=filename to start SQL!"<<endl;
        exit(1);
    }
    string query = "";
    for(int i = 1 ; i < argc; i ++ ){
        query += argv[i];
        query += " ";
    }

    loadTableList(table_list);

    ofstream myfile;
    myfile.open (output_file);
    myfile.close();

    if(query.find("script=")== 0)
    {
        //transcation part
        if(query.find("numthreads")){
            cout<<"Start transaction"<<endl;
            string temp = "";
            string filename = "";
            string pkcolumn = "";
            int num_threads = 10;

            for(auto p : query.substr(7)){

                if(p == ';' | p == ' '){
                    if(filename == ""){
                        filename = temp;
                    }
                    else if(temp.find("numthreads") == 0){
                        num_threads = stoi(temp.substr(11));
                    }
                    else if(temp.find("pkcolumn") == 0){
                        pkcolumn = temp.substr(9);
                    }
                    temp = "";
                }
                else
                    temp += p;
            }
            temp = "";


            //read script sql build thread




            ThreadPool threads(num_threads);
            string transbuffer="";
            //read .sql file to transcations
            ifstream file(filename);
            if(!file.is_open()){
                cout<< "file cannot opened"<<endl;
            }
            string buffer="";
            while(getline(file, temp)){
                string rs_temp = removespace(temp);
                if(rs_temp.length() <= 1)
                    continue;
                Lowercase(temp);
                //if(util::compareString(temp.substr(temp.length()-1), ";") == 1){
                if(temp.find(";") != string::npos){
                    buffer += " " + temp;
                    //deal with transcation
                    if(buffer.find("begin transaction") != string::npos){
                        string t_temp = "";
                        //filter out BEGIN TRANSACTION
                        transbuffer = buffer.substr(18);
                        vector<string> lockitem;
                        lockitem.clear();
                        //first where
                        if(transbuffer.find("where") != string::npos){
                            string substring = transbuffer.substr(transbuffer.find("where") + 6);
                            substring = substring.substr(0, substring.find(";"));
                            vector<string> split_string = split(substring, '=');
                            lockitem.push_back(removespace(split_string[0]) + removespace(split_string[1]));
                        }
                        while(getline(file, t_temp)){
                            rs_temp = removespace(t_temp);
                            if(rs_temp.length() <= 1)
                                continue;
                            Lowercase(t_temp);
                            transbuffer = transbuffer +  " " + t_temp;
                            if(t_temp.find("where") != string::npos){
                                string substring = t_temp.substr(t_temp.find("where") + 6);
                                substring = substring.substr(0, substring.find(";"));
                                vector<string> split_string = split(substring, '=');
                                if(find(lockitem.begin(), lockitem.end(), removespace(split_string[0] + removespace(split_string[1]))) == lockitem.end())
                                    lockitem.push_back(removespace(split_string[0]) + removespace(split_string[1]));
                            }
                            if(transbuffer.find("end transaction") != string::npos){
                                //cout << "execute transaction" << endl;
                                //cout << transbuffer << endl;
                                checkthread.push(1);
                                threads.doJob(bind(executeTransaction, transbuffer, lockitem));
                                transbuffer = "";
                                break;
                            }
                        }

                    }
                    else{
                        //check if all threads stoped
//                        for(auto item: locks){
//                            while(true){
//                                cout << item.second << endl;
//                                if(item.second == 0)
//                                    break;
//                                cout << "locks on " << item.first << endl;
//                            }
//
//                        }
                        //execute query directly
//
                        while(!checkthread.empty()){
                            //wait;
                        }
                        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(buffer);

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
                    }

                    buffer = "";
                } else
                    buffer += " " + temp;

            }


            return 0;
        }

        //read script and run sql by script
        //cout << "run script " << endl;
        ifstream file(query.substr(7, query.length()-8));
        if(!file.is_open())
        {
            cout << "file cannot opened" <<endl;
        }
        stringstream buffer;
        buffer << file.rdbuf();
        string wholequery = "";
        for(auto c : buffer.str()){
            if(c != '\r' )
                wholequery += c;
        }
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(wholequery);
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

        return 0;


    }
    else{
        query += ";";
        while(true) {

            // printTableList(table_list);

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
    }

    return 0;
}

void executeStatement(hsql::SQLStatement *stmt, map<string, table*> &table_list){
    switch (stmt->type()) {
        case hsql::kStmtCreate:
            //cout << "Create" <<endl;
            createTable((hsql::CreateStatement*)stmt, table_list);
            break;
        case hsql::kStmtSelect:
            //cout << "Select" <<endl;
            executeSelect((hsql::SelectStatement*)stmt, table_list);
            break;
        case hsql::kStmtInsert:
            //cout << "Insert" <<endl;
            insertTable((hsql::InsertStatement*)stmt, table_list, false);
            break;
        case hsql::kStmtShow:
            //cout << "Show" <<endl;
            executeShow((hsql::ShowStatement*)stmt, table_list);
            break;
        case hsql::kStmtUpdate:
            //cout << "Update" <<endl;
            executeUpdate((hsql::UpdateStatement*)stmt, table_list, false);
        default:
            break;
    }
}



void printTableList(map<string, table*> table_list){

    if(table_list.empty()) cout << "nothing" << endl;
    else cout << table_list.begin()->first <<endl;

    for(map<string, table*>::const_iterator it = table_list.begin();
        it != table_list.end(); ++it)
    {
        std::cout << it->first << " " << it->second->getRowlength() << " " << it->second->getPrimaryKey() << "\n";
    }
}


void createTable(hsql::CreateStatement *stmt, map<string, table*> &table_list){
    cout << "Creating table " << stmt->tableName << "... " <<endl;
    //chect duplicate columns
    vector<char*> colnames;
    for(hsql::ColumnDefinition* col_def: *stmt->columns){
        for(auto colname:colnames){
            if(util::compareString(colname, col_def->name)){
                cout<<"Can't create table with duplicate column names"<<endl;
                return;
            }
        }
        colnames.push_back(col_def->name);
    }
    //check if table exist
    for(auto t:table_list){
        if(util::compareString(t.second->getabsName(), stmt->tableName)){
            cout << "table "<<stmt->tableName << " already exists"<<endl;
            return;
        }
    }
    table* newtable = new table(stmt->tableName);
    vector<column* > cols;
    //put cols to table
    int row_length = 0;
    for(hsql::ColumnDefinition* col_def : *stmt->columns){
        string flag ;
        int size = 0;
        if (col_def-> type == hsql::ColumnDefinition::DataType::INT){
            flag = "INT";
            size = 8;
            row_length += 8;
        }
        else if (col_def->type == hsql::ColumnDefinition::DataType::TEXT){
            flag = "CHAR";
            size = col_def->size;
            row_length += size;
            row_length += 1;
        }
        column* newcol = new column(col_def->name, flag, size);
        cols.push_back(newcol);
    }
    newtable->addColumn(cols);
    newtable->setRowSize(row_length);
    ofstream os(newtable->getName(), ios::out | ios::binary);

    table_list.insert(make_pair(stmt->tableName, newtable));

    //set primary key
    if(stmt->primaryKey != NULL){
        newtable->setPrimaryKey(stmt->primaryKey->name);
    }
    os.close();

}
bool insertTable(hsql::InsertStatement *stmt, map<string, table*> &table_list, bool check){
    cout<<"Insert into table : "<< stmt->tableName << endl;

    table* totable = util::getTable(stmt->tableName, table_list);
    if(totable == NULL){
        cout<<"table "<<stmt->tableName<<" not exits"<<endl;
        return false;
    }

    if (stmt->type == hsql::InsertStatement::kInsertValues&& !check){
        if(totable->insert(stmt)){
            cout << "insert successful" << endl;
            return true;
        }
    }
    //check insert
    if (stmt->type == hsql::InsertStatement::kInsertValues&&check){
        if(totable->insertcheck(stmt)){
            return true;
        }
    }

    if (stmt->type == hsql::InsertStatement::kInsertSelect){
        if(totable->insertSelect(stmt, table_list)){
            cout << "insert successful" << endl;
            return true;
        }
    }
    return false;

}


void executeShow(hsql::ShowStatement *stmt, map<string, table*> &table_list){
    ifstream is("CATALOG.txt");
    string line;
    if(stmt->tableName ){
        while(getline(is, line)){
                if (stmt->tableName == line.substr(10)){
                    cout << "Table " << line.substr(10) << "(" ;
                    // columns
                    getline(is, line);
                    cout << line.substr(8) ;
                    //primaryKey
                    getline(is, line);
                    cout << "," << line << ")"<< endl;
                    getline(is,line);
                    getline(is, line);
                    getline(is, line);
                }
                else{
                    getline(is, line);
                    getline(is, line);
                    getline(is, line);
                    getline(is, line);
                    getline(is, line);
                }
        }
    }
    else {
        while(getline(is, line)){
            //table name
            cout << "Table " << line.substr(10) << "(" ;
            // columns
            getline(is, line);
            cout << line.substr(8) ;
            //primaryKey
            getline(is, line);
            cout << "," << line << ")"<< endl;
            //recordsize
            getline(is,line);
            // total size
            getline(is, line);
            // records
            getline(is, line);
        }
    }

}

void executeSelect(hsql::SelectStatement *stmt, map<string, table*> &table_list){
    if(stmt->fromTable->type == hsql::kTableName){
        table* totable = util::getTable(stmt->fromTable->name, table_list);
        if(totable == NULL){
            cout<< "did not find table " << stmt->fromTable->name << " from database"<<endl;
            return;
        }

        if(totable != nullptr) {
            //totable->select(stmt);
            int selectvalue = 0;
            util::PrintRecords(stmt, totable->select(stmt), totable, selectvalue);
        }
    }
    else{//do join
        cout<< "join" <<endl;
        char* leftname = stmt->fromTable->join->left->getName();
        char* rightname =  stmt->fromTable->join->right->getName();
        table* lefttable = util::getTable(leftname, table_list);
        table* righttable = util::getTable(rightname, table_list);
        if(lefttable==nullptr)
            cout << "table "<< leftname << " do not exist" << endl;
        if (righttable == nullptr)
            cout << "table "<< rightname << " do not exist" << endl;
        if(lefttable != nullptr && righttable != nullptr){
            //join
            joinTable(lefttable, righttable, stmt);
        }

    }

}
void joinTable(table* t1, table* t2, hsql::SelectStatement *stmt){

    vector<pair<string, column*>> cols_left;
    vector<pair<string, column*>> cols_right;

    cols_left = t1->select(stmt);
    cols_right = t2->select(stmt);

    util::PrintJoinRecords(stmt, cols_left, cols_right, t1, t2);


}

bool executeUpdate(hsql::UpdateStatement *stmt, map<string, table*> &table_list, bool check){
    if(stmt->table->type == hsql::kTableName) {
        table *totable = util::getTable(stmt->table->name, table_list);
        if (totable == NULL) {
            cout << "did not find table " << stmt->table->name << " from database" << endl;
            return false;
        }
        if(totable != nullptr){
            if(!check){
                if(totable->update(stmt)){
                    //cout << "update successful" << endl;

                    return true;
                }
            } else{
                if(totable->updatecheck(stmt)){
                    return true;
                }
            }


        }
    }
    //cout << "update false"<<endl;
    return false;
}

//need more founctions

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
            if(col->flag == "INT") temp_str += col->name + ":" + col->flag + ",";
            else temp_str += col->name + ":" + col->flag + "("+ to_string( col->element_size) + "),";
        }
        os << temp_str.substr(0, temp_str.size() - 1 ) << endl;

        if(tl.second->getPrimaryKey()!= NULL)
            os << "primaryKey=" << tl.second->getPrimaryKey()->name << endl;
        else
            os << "primaryKey=NULL" << endl;
        os << "recordsize="<<tl.second->getRecordSize() << endl;
        os << "totalrecordsize="<<tl.second->getTotalRecordSize() << endl;
        os << "records="<< tl.second->getRowlength()<< endl;
    }
    os.close();
}

void loadTableList(map<string, table*> &table_list){
    ifstream is("CATALOG.txt");
    string line;
    while(getline(is, line)){
        string tableName = line.substr(10);
        table* t = new table(tableName );
       // cout << line.substr(10) << endl;
        getline(is, line);

        vector<column*> columns;
        string strCol = line.substr(8);
        vector<string> tokens = split(strCol, ',');
        int rowSize = 0;
        for(auto col_str:tokens) {
            column *col = new column();
            col->fromString(col_str);
            rowSize += col->element_truesize;
            columns.push_back(col);
        }
        t->addColumn(columns);

        // primary key
        getline(is, line);
        t->setPrimaryKey(line.substr(11));

        getline(is, line);
        // total size
        getline(is, line);

        // records
        getline(is, line);
        t->setRowLength(stoi(line.substr(8)));

        t->setRowSize(rowSize);

        table_list.insert(make_pair(tableName, t));
    }

    is.close();
}

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
void Lowercase(string &s){
    //for (int i=0; s[i]; i++) s[i] = tolower(s[i]);
    int i = 0;
    char c;
    while(s[i]){
        if(s[i] <='Z' && s[i] >= 'A'){
            c = s[i];
            s[i] = tolower(c);
            i++;
            continue;
        }
        if(s[i] == '\r')
            s[i] = ' ';
        i ++;
    }
}
string removespace(string s){
    string temp;
    for (int i=0; s[i];i++){
        if(s[i] == ' ' || s[i] == '\r' )
            continue;
        temp += s[i];
    }
    return temp;
}

void executeTransaction(string transbuffer, vector<string> lockitem){
//    //check queue if thread in or out
    //lock on the record
//    con.lock();
//    for(auto item: lockitem) {
//        int sleep_time = 1000;
//        while (true) {
//            //con.lock();
//            //cout << item << "condition is : " << locks[item]<<endl;
//            if (locks.find(item) != locks.end() && locks[item] == 0) {
//                locks[item] = 1;
//                //con.unlock();
//                break;
//            }
//            if (locks.find(item) == locks.end()) {
//                locks.insert(make_pair(item, 1));
//                //con.unlock();
//                break;
//            }
//
//        }
//
//        this_thread::sleep_for(chrono::milliseconds(sleep_time));
//        sleep_time *= 2;
//    }
//
//    con.unlock();

    int sleep_time = 1000;
    int sleep_count = 0;
    while(true){
        if(check_true_size(lockitem)){
            break;
        }
        sleep_count++;
        //int v2 = rand() % 1000 + 1;
        this_thread::sleep_for(chrono::milliseconds(sleep_time));
        if(sleep_count > 3){
            sleep_time = 1000;
        }
        else{
            sleep_time *= 2;
        }
    }



    //threads_id.push(this_thread::get_id());

//    string previous_item = "";
//
//    //lock
//    con.lock();
//    while(true){
//
//        int true_size = 0;
//        map<string, int> temp_locks;
//
//        for(auto item: lockitem) {
//
//            if(item == previous_item){
//                previous_item = item;
//                true_size++;
//                continue;
//            }
//            else if(locks.find(item) != locks.end() && locks[item] == 0)
//            {
//                temp_locks[item] = 1;
//                locks[item] = 1;
//                true_size++;
//                previous_item = item;
//                continue;
//            }
//
//            else if(locks.find(item) == locks.end()){
//
//                temp_locks[item] = 1;
//                locks.insert(make_pair(item, 1));
//                true_size++;
//                previous_item = item;
//                continue;
//            }
//            previous_item = item;
//        }
//        if(true_size == lockitem.size()){
//            con.unlock();
//            break;
//        }
//        else {
//            for (std::map<string, int>::iterator it = temp_locks.begin(); it != temp_locks.end(); ++it) {
//                if (it->second == 1) {
//                    locks[it->first] = 0;
//                }
//            }
//        }
//        //int v2 = rand() % 1000 + 1;
//        this_thread::sleep_for(chrono::milliseconds(1000));
//    }

    //unlock
    //con.unlock();

    //check if the transbuffer can be executed.
    vector<string> querys = split(transbuffer, ';');
    bool cancommit = true;
    vector<string> update_query;

    for(auto query : querys){

        string t_query = removespace(query);
        if(t_query.length() < 1)
            continue;
        if(query.find("end transaction") != string::npos || query.find("commit") != string::npos)
            continue;

        query += ";";
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);
        // check whether the parsing was successful
        if (result->isValid()) {
            for (unsigned i = 0; i < result->size(); ++i) {
                //run sql query


                //check insert
                if(query.find("insert") != string::npos){

                    hsql::InsertStatement *stmt = (hsql::InsertStatement*)result->getMutableStatement(i);
                    table* totable = util::getTable(stmt->tableName, table_list);
                    if(totable == NULL)
                        cancommit = false;
                    if(totable != NULL && !totable->insertcheck(stmt))
                        cancommit = false;

                }

                if(query.find("update") != string::npos){
                    hsql::UpdateStatement *stmt = (hsql::UpdateStatement*)result->getMutableStatement(i);
                    table* totable = util::getTable(stmt->table->name, table_list);
                    if(totable == NULL)
                    {
                        cout << "cannot update this statement"<< endl;
                        cancommit = false;
                    }
                    if(totable != NULL && !totable->updatecheck(stmt))
                    {
                        cout << "cannot update this statement"<< endl;
                        cancommit = false;
                    }

                }


            }
        } else {

            //split update to two querys and check again
            if(query.find("update") != string::npos ){
                if(query.find("insert into") != string::npos )
                    cancommit = false;;
                string select_item = query.substr(query.find("set") + 3, query.find("where") - query.find("set") - 3);
                select_item = split(select_item, '=')[0];
                string stable = query.substr(query.find("update") + 6, query.find("set") - query.find("update") -6);
                string where = query.substr(query.find("where") + 5, query.find(";") - query.find("where") - 5);
                string operation = "";
                if(query.find("-")!= string::npos)
                    operation="-";
                else
                    operation="+";


                string select_query = "select " + select_item + " from " + stable + " where " + where + ";";


                //select query
                int selectvalue = -1;
                hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(select_query);
                if (result->isValid()) {
                    for (unsigned i = 0; i < result->size(); ++i) {
                        //run sql query
                        hsql::SelectStatement *stmt = (hsql::SelectStatement*)result->getMutableStatement(i);
                        if(stmt->fromTable->type == hsql::kTableName){
                            table* totable = util::getTable(stmt->fromTable->name, table_list);
                            if(totable == NULL){
                                cout<< "did not find table " << stmt->fromTable->name << " from database"<<endl;
                                cancommit = false;
                            }

                            if(totable != nullptr) {
                                util::PrintRecords(stmt, totable->select(stmt), totable, selectvalue);
                                if(operation == "-")
                                    selectvalue -= 1;
                                else
                                    selectvalue += 1;
                                if(selectvalue < 0)
                                    cancommit = false;

                            }
                        }

                    }
                } else {
                    //cout << "Given string is not a valid SQL query." << endl
                     //    << result->errorMsg() << "(" << result->errorLine() << ":" << result->errorColumn() << ")" << endl;
                    cancommit = false;
                }
                //update query
                string temp_update_query = "update " + stable + " set " + select_item + " = " + to_string(selectvalue) + " where " + where + ";";
                update_query.push_back(temp_update_query);
                result = hsql::SQLParser::parseSQLString(temp_update_query);
                if (result->isValid()) {
                    for (unsigned i = 0; i < result->size(); ++i) {
                        //run sql query
                        hsql::UpdateStatement *stmt = (hsql::UpdateStatement*)result->getMutableStatement(i);
                        if(!executeUpdate(stmt, table_list, true)){
                            cancommit = false;
                        }
                    }
                } else {
                    //cout << "Given string is not a valid SQL query." << endl
                     //    << result->errorMsg() << "(" << result->errorLine() << ":" << result->errorColumn() << ")" << endl;
                    cancommit = false;
                }


            }
            else{
                //cout << "Given string is not a valid SQL query." << endl
                 //    << result->errorMsg() << "(" << result->errorLine() << ":" << result->errorColumn() << ")" << endl;
                cancommit = false;
            }


        }

        //cout << "find query" << query << endl;
        if(cancommit == false)
            break;
    }


    //commit
    int numberOfRowsModified = 0;
    if(cancommit){
        //cout << "do commit" << endl;
        int countupdate = 0;
        for(auto query: querys){
            //cout << "query: " << query << endl;
            string t_query = removespace(query);
            if(t_query.length() < 1)
                continue;
            if(query.find("end transaction") != string::npos || query.find("commit") != string::npos)
                continue;

            query += ";";
            hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);

            // check whether the parsing was successful
            if (result->isValid()) {
                for (unsigned i = 0; i < result->size(); ++i) {
                    //run sql query
                    executeStatement(result->getMutableStatement(i), table_list);
                    saveToFile(table_list);

                }
            } else {
                if(query.find("update") != string::npos){
                    numberOfRowsModified++;
                    query = update_query[countupdate];
                    countupdate += 1;
                }
                hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);

                // check whether the parsing was successful
                if (result->isValid()) {
                    for (unsigned i = 0; i < result->size(); ++i) {
                        //run sql query
                        executeStatement(result->getMutableStatement(i), table_list);
                        saveToFile(table_list);
                    }
                }
                else{
                    cout << "Given string is not a valid SQL query." << endl
                         << result->errorMsg() << "(" << result->errorLine() << ":" << result->errorColumn() << ")" << endl;
                }

            }

        }
    }

    //execute the transbuffr
    unlock_items(lockitem, numberOfRowsModified);



}

bool check_true_size(vector<string> lockitem){
    con.lock();
    int true_size = 0;
    map<string, int> temp_locks;
    string previous_item = "";
    for(auto item: lockitem) {
        if(item == previous_item){
            previous_item = item;
            true_size++;
            continue;
        }
        else if(locks.find(item) != locks.end() && locks[item] == 0)
        {
            temp_locks[item] = 1;
            locks[item] = 1;
            true_size++;
            previous_item = item;
            continue;
        }

        else if(locks.find(item) == locks.end()){

            temp_locks[item] = 1;
            locks.insert(make_pair(item, 1));
            true_size++;
            previous_item = item;
            continue;
        }
        previous_item = item;
    }
    if( true_size == lockitem.size()){
        //output affected rows
        //ofstream myfile(output_file, ios::app);
        //if(myfile.is_open()){
            for(auto item: lockitem){
                //myfile << "Row " << item <<" locked\n" ;
                cout << "Row " << item <<" locked\n" ;
            }
            //myfile.close();
        //}
        con.unlock();
        return true;
    }
    else {
        for (std::map<string, int>::iterator it = temp_locks.begin(); it != temp_locks.end(); ++it) {
            if (it->second == 1) {
                locks[it->first] = 0;
            }
        }
        con.unlock();
        return false;
    }
    con.unlock();
    return false;
}

void unlock_items(vector<string> lockitem, int x){
    //unlock
    con.lock();
    for(auto item:lockitem){
        locks[item] = 0;
    }
    checkthread.pop();

    //output affected rows
    //ofstream myfile(output_file, ios::app);
    //if(myfile.is_open()) {
        //myfile << x <<" Row(s) Modified\n";
        cout << x <<" Row(s) Modified\n";
        for(auto item: lockitem){
            //myfile << "Row " << item <<" unlocked\n";
            cout << "Row " << item <<" unlocked\n";
        }
        //myfile.close();
    //}
    con.unlock();
}