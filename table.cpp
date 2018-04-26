#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include "table.h"
#include "util.h"
#include <iomanip>
#define TABLE_PATH "./"
using namespace std;

table::table(){

}
table::table(const string& name){
    //get table name
    filename = TABLE_PATH + name + ".tbl";
    absname = name;
}
table::~table(){

}

void table::addColumn(vector<column*> &cols){
    table_cols = cols;
    int element_true_size = 0;
    int element_size = 0;
    for(auto col: cols){
        col->col_offset = element_true_size;
        element_true_size += col->element_truesize;
        element_size += col->element_size;
    }
    recordSize = element_size;
}

void table::setPrimaryKey(const string &name) {
    primaryKey = getColumn(name);
}

column* table::getColumn(const string &name){
    for(auto col: table_cols){
        if(util::compareString(col->name, name))
            return col;
    }
    return NULL;
}

bool table::insert(hsql::InsertStatement *stmt) {
    if(stmt->columns != NULL && stmt->columns->size() != stmt->values->size()){
        cout <<"Column count doesn't match value count"<<endl;
        return false;
    }
    if(stmt->columns == NULL && table_cols.size() != stmt->values->size()){
        cout <<"INSERT has different number of columns than target columns"<<endl;
        return false;
    }
    if(stmt->columns != NULL){
        for(char* col_name : *stmt->columns){
            auto it = getColumn(col_name);
            if(it == NULL){
                cout <<"Unknown column "<<col_name<<" in table list'"<<endl;
                return false;
            }
        }
    }
    fstream os(filename, ios::in|ios::out|ios::binary|ios::app);
    if(!os.is_open()){
        cout << "cannot open table file" <<endl;
        return false;
    }

    //check insert type
    column* primary = getPrimaryKey();
    for(int i=0; i < table_cols.size(); i++){

        if(table_cols[i]->flag == "CHAR"){
            if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                const char* str = (*stmt->values)[i]->name ;
                if( strlen(str) > table_cols[i]->element_truesize ){
                    cout << "Wrong Char size" << endl;
                    return false;
                }
                if(primary != nullptr && util::compareString(table_cols[i]->name, primary->name)){
                    cout << "check primary key values"<<endl;
                    for(int j=0 ; j < getRowlength(); j++){
                        os.seekg(j * rowSize + table_cols[i]->col_offset);
                        char *bytes = new char[table_cols[i]->element_truesize];
                        os.read(bytes, table_cols[i]->element_truesize);
                        if(util::compareString(bytes, str)){
                            cout<<"Can't insert duplicate values to primary key column"<<endl;
                            delete bytes;
                            return false;
                        }

                    }
                }

            }
            else{
                const char* str = to_string( (*stmt->values)[i]->ival ).c_str();
                if( strlen(str) > table_cols[i]->element_truesize ){
                    cout << "Wrong Char size" << endl;
                    return false;
                }
                if(primary!=nullptr&&util::compareString(table_cols[i]->name, primary->name)){
                    for(int j=0 ; j < getRowlength(); j++){
                        os.seekg(j * rowSize + table_cols[i]->col_offset);
                        char *bytes = new char[table_cols[i]->element_truesize];
                        os.read(bytes, table_cols[i]->element_truesize);
                        if(util::compareString(bytes, str)){
                            cout<<"Can't insert duplicate values to primary key column"<<endl;
                            delete bytes;
                            return false;
                        }
                    }
                }
            }
        }
        else{
            if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                cout<<"need a int value instead of a string"<<endl;
                return false;
            }
            if(primary!= nullptr && util::compareString(table_cols[i]->name, primary->name)){
                for(int j=0 ; j < getRowlength(); j++){
                    os.seekg(j * rowSize + table_cols[i]->col_offset);
                    char *bytes = new char[table_cols[i]->element_truesize];
                    os.read(bytes, table_cols[i]->element_truesize);
                    //cout  << bytes <<endl;
                    if(util::compareString(bytes, (char*)&(*stmt->values)[i]->ival)){
                        cout<<"Can't insert duplicate values to primary key column"<<endl;
                        delete bytes;
                        return false;
                    }

                }
            }

        }
    }

    //do insert
    for(int i = 0; i < table_cols.size(); i++){
        if(stmt->columns != NULL){
        //insert value to selected columns
        }
        else{
            if(table_cols[i]->flag == "CHAR"){
                if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                    const char* str = (*stmt->values)[i]->name ;
                    os.write(str,  table_cols[i]->element_truesize ) ;

                }
                else{
                    const char* str = to_string( (*stmt->values)[i]->ival ).c_str();
                    os.write(str,  table_cols[i]->element_truesize ) ;
                }
            }
            else{
                os.write((char*)&(*stmt->values)[i]->ival, 8) ;

            }

        }
    }
    rowlength++;
    os.close();
    return true;

}


vector<pair<string, column*>> table::select(hsql::SelectStatement *stmt){

    vector<pair<string, column*>> cols;

    if(stmt->selectList->size() == 1 && (*stmt->selectList)[0]->type == hsql::kExprStar) {    // select *
        for (auto col : table_cols)
            cols.push_back(make_pair(col->name, col));
    }
    else{
        for(hsql::Expr* expr : *stmt->selectList){

            if(expr->type == hsql::kExprLiteralString || expr->type == hsql::kExprColumnRef){   // select C1,C2,...
                string colName = expr->name;
                column *col = getColumn(colName);
                if(col == NULL){
                    cout <<"Column '"<<colName<<"' does not exist in table "<<getabsName()<<endl;
                    return {};
                }else{
                    cols.push_back(make_pair(col->name, col));
                }
            }//else if(expr->type == hsql::kExprLiteralInt){
            //    cols.push_back(make_pair(to_string(expr->ival), (column*)NULL));
            //}
        }
    }
    return cols;

}

bool table::update(hsql::UpdateStatement *stmt){

    column* col = getColumn((*stmt->updates)[0]->column);
    bool updated = false;
    if(col == NULL){
        cout << "no such column in table to update" <<endl;
        return false;
    }
    if(stmt->where == nullptr){
        //set all column to same value
    }
    else {
        fstream os(getName(), ios::in|ios::out|ios::binary);

        for (int i = 0; i < getRowlength(); i++) {
            if (stmt->where->expr2->type == hsql::kExprLiteralInt || stmt->where->expr->type == hsql::kExprLiteralInt) {
                //select column
                string colName;
                int compareNum;
                if (stmt->where->expr2->type == hsql::kExprLiteralInt) {
                    if (stmt->where->expr->type == hsql::kExprLiteralString) {
                        cout << "cannot do compare between int and char" << endl;
                        return false;
                    }
                    colName = stmt->where->expr->name;
                    compareNum = (int) stmt->where->expr2->ival;
                } else {
                    if (stmt->where->expr2->type == hsql::kExprLiteralString) {
                        cout << "cannot do compare between int and char" << endl;
                        return false;
                    }
                    colName = stmt->where->expr2->name;
                    compareNum = (int) stmt->where->expr->ival;
                }
                column *tempcol = NULL;
                //check if table have this column
                for (auto col: table_cols) {
                    if (colName == col->name) {
                        tempcol = col;
                    }
                }
                if (tempcol == NULL) {
                    cout << "column " << colName << " do not exist in table" << endl;
                    return false;
                }
                //get column value
                os.seekg(i * getrowSize() + tempcol->col_offset);
                char *bytes = new char[tempcol->element_truesize];
                os.read(bytes, tempcol->element_truesize);
                //check =
                if (stmt->where->opChar == '=') {
                    if (tempcol->flag == "CHAR") {
                        cout << "cannot do = for char columns with int value" << endl;
                        delete bytes;
                        return false;
                    }
                    if (*(int *) bytes != compareNum) {
                        delete bytes;
                        continue;
                    }
                }
                //check >
                if (stmt->where->opChar == '>') {
                    if (tempcol->flag == "CHAR") {
                        cout << "cannot do > for char columns with int value" << endl;
                        delete bytes;
                        return false;
                    }
                    if (*(int *) bytes <= compareNum) {
                        delete bytes;
                        continue;
                    }
                }
                //check <
                if (stmt->where->opChar == '<') {
                    if (tempcol->flag == "CHAR") {
                        cout << "cannot do < for char columns with int value" << endl;
                        delete bytes;
                        return false;
                    }
                    if (*(int *) bytes >= compareNum) {
                        delete bytes;
                        continue;
                    }
                }

            }
            if (stmt->where->expr->type == hsql::kExprColumnRef && stmt->where->expr2->type == hsql::kExprColumnRef)
                continue;
            //do char
            if (stmt->where->expr2->type == hsql::kExprLiteralString ||
                stmt->where->expr->type == hsql::kExprLiteralString) {
                //select column
                string colName;
                char *compareChar;
                if (stmt->where->expr2->type == hsql::kExprLiteralString) {
                    colName = stmt->where->expr->name;
                    compareChar = stmt->where->expr2->name;
                } else {
                    colName = stmt->where->expr2->name;
                    compareChar = stmt->where->expr->name;
                }
                column *tempcol = NULL;
                //check if table have this column
                for (auto col: table_cols) {
                    if (colName == col->name) {
                        tempcol = col;
                    }
                }
                if (tempcol == NULL) {
                    cout << "column " << colName << " do not exist in table" << endl;
                    return false;
                }
                //get column value
                os.seekg(i * getrowSize() + tempcol->col_offset);
                char *bytes = new char[tempcol->element_truesize];
                os.read(bytes, tempcol->element_truesize);
                //check =
                if (stmt->where->opChar == '=') {
                    if (!util::compareString(bytes, compareChar)) {
                        delete bytes;
                        continue;
                    }
                }
                    //check >
                else if (stmt->where->opChar == '>') {
                    if (util::compareChar(bytes, compareChar) != 1) {
                        delete bytes;
                        continue;
                    }
                }
                    //check <
                else if (stmt->where->opChar == '<') {
                    if (util::compareChar(bytes, compareChar) != -1) {
                        delete bytes;
                        continue;
                    }
                }

            }
            //do update

            if (col != NULL) {

                if (col->flag == "CHAR") {
                    if (((*stmt->updates)[0]->value)->type == hsql::kExprLiteralString) {
                        const char *str = ((*stmt->updates)[0]->value)->name;
                        os.seekg(i * getrowSize() + col->col_offset);
                        os.write(str, col->element_truesize);
                        updated = true;

                    } else {
                        const char *str = to_string(((*stmt->updates)[0]->value)->ival).c_str();
                        os.seekg(i * getrowSize() + col->col_offset);
                        os.write(str, col->element_truesize);
                        updated = true;
                    }
                } else {
                    os.seekg(i * getrowSize() + col->col_offset);
                    os.write((char *) &((*stmt->updates)[0]->value)->ival, 8);
                    updated = true;

                }
            }

        }
        os.close();
    }
    if(updated)
        return true;
    else{
        cout<<"where condition is wrong"<<endl;
        return false;
    }


}
bool table::insertSelect(hsql::InsertStatement *stmt, map<string, table*> &table_list) {
    table* fromtable = util::getTable(stmt->select->fromTable->name, table_list);
    if(fromtable == nullptr){
        cout << "table: "<<stmt->select->fromTable->name<<" exist in database"<<endl;
        return false;
    }
    vector<pair<string, column*>> selected_cols = fromtable->select(stmt->select);

    if(!selected_cols.empty()  && table_cols.size() != selected_cols.size()){
        cout <<"INSERT has different number of columns than target columns"<<endl;
        return false;
    }
    if(selected_cols.empty()){
        return false;
    }


    //check table type
    for(int i = 0; i < table_cols.size(); i++){
        if(table_cols[i]->flag != selected_cols[i].second->flag){
            cout << "table " << fromtable->getabsName() << " have different columns as table " << getabsName() << endl;
            return false;
        }
        if(table_cols[i]->flag == "CHAR"){
            if(table_cols[i]->element_truesize < selected_cols[i].second->element_truesize){
                cout << "target table char size can't fit" <<endl;
                return false;
            }
        }

    }
    column* primary = getPrimaryKey();
    fstream os(filename, ios::in|ios::out|ios::binary|ios::app);
    ifstream os_from(fromtable->getName(), ios::in|ios::binary);

    if(!os.is_open()){
        cout << "cannot open table file" <<endl;
        return false;
    }
    if(!os_from.is_open()){
        cout << "cannot open select table file"<<endl;
        return false;
    }

    for(int i = 0; i < fromtable->getRowlength(); i++){
        //check if this row can be inserted
        bool caninsert = true;
        for(int j=0; j < table_cols.size(); j++){
            os_from.seekg(i * fromtable->getrowSize() + selected_cols[j].second->col_offset);
            char* str = new char[selected_cols[j].second->element_truesize];
            os_from.read(str, selected_cols[j].second->element_truesize);
            if(primary != nullptr && util::compareString(table_cols[j]->name, primary->name)){
                for(int k=0 ; k < getRowlength(); k++) {
                    os.seekg(k * rowSize + table_cols[j]->col_offset);
                    char *bytes = new char[table_cols[j]->element_truesize];
                    os.read(bytes, table_cols[j]->element_truesize);
                    if (util::compareString(bytes, str)) {
                        cout << "Can't insert duplicate values to primary key column" << endl;
                        //delete bytes;
                        caninsert = false;
                    }
                    delete bytes;
                }
            }
            delete str;
        }
        //do insert
        if(caninsert){
            for(int j=0; j < table_cols.size(); j++){
                os_from.seekg(i * fromtable->getrowSize() + selected_cols[j].second->col_offset);
                char* str = new char[selected_cols[j].second->element_truesize];
                os_from.read(str, selected_cols[j].second->element_truesize);
//
//                char* bytes = new char[table_cols[j]->element_truesize];
//                for(int z = 0 ; z < strlen(str); z++){
//                    bytes[z] = str[z];
//                }
                os.write(str, table_cols[j]->element_truesize);

                delete str;

            }
            rowlength++;
        }
    }

    os.close();
    os_from.close();
    return true;

}

//insert check
bool table::insertcheck(hsql::InsertStatement *stmt) {

    if(stmt->columns != NULL && stmt->columns->size() != stmt->values->size()){
        cout <<"Column count doesn't match value count"<<endl;
        return false;
    }
    if(stmt->columns == NULL && table_cols.size() != stmt->values->size()){
        cout <<"INSERT has different number of columns than target columns"<<endl;
        return false;
    }
    if(stmt->columns != NULL){
        for(char* col_name : *stmt->columns){
            auto it = getColumn(col_name);
            if(it == NULL){
                cout <<"Unknown column "<<col_name<<" in table list'"<<endl;
                return false;
            }
        }
    }
    fstream os(filename, ios::in|ios::out|ios::binary|ios::app);
    if(!os.is_open()){
        cout << "cannot open table file" <<endl;
        return false;
    }

    //check insert type
    column* primary = getPrimaryKey();
    for(int i=0; i < table_cols.size(); i++){

        if(table_cols[i]->flag == "CHAR"){
            if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                const char* str = (*stmt->values)[i]->name ;
                if( strlen(str) > table_cols[i]->element_truesize ){
                    cout << "Wrong Char size" << endl;
                    return false;
                }
                if(primary != nullptr && util::compareString(table_cols[i]->name, primary->name)){
                    cout << "check primary key values"<<endl;
                    for(int j=0 ; j < getRowlength(); j++){
                        os.seekg(j * rowSize + table_cols[i]->col_offset);
                        char *bytes = new char[table_cols[i]->element_truesize];
                        os.read(bytes, table_cols[i]->element_truesize);
                        if(util::compareString(bytes, str)){
                            cout<<"Can't insert duplicate values to primary key column"<<endl;
                            delete bytes;
                            return false;
                        }

                    }
                }

            }
            else{
                const char* str = to_string( (*stmt->values)[i]->ival ).c_str();
                if( strlen(str) > table_cols[i]->element_truesize ){
                    cout << "Wrong Char size" << endl;
                    return false;
                }
                if(primary!=nullptr&&util::compareString(table_cols[i]->name, primary->name)){
                    for(int j=0 ; j < getRowlength(); j++){
                        os.seekg(j * rowSize + table_cols[i]->col_offset);
                        char *bytes = new char[table_cols[i]->element_truesize];
                        os.read(bytes, table_cols[i]->element_truesize);
                        if(util::compareString(bytes, str)){
                            cout<<"Can't insert duplicate values to primary key column"<<endl;
                            delete bytes;
                            return false;
                        }
                    }
                }
            }
        }
        else{
            if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                cout<<"need a int value instead of a string"<<endl;
                return false;
            }
            if(primary!= nullptr && util::compareString(table_cols[i]->name, primary->name)){
                for(int j=0 ; j < getRowlength(); j++){
                    os.seekg(j * rowSize + table_cols[i]->col_offset);
                    char *bytes = new char[table_cols[i]->element_truesize];
                    os.read(bytes, table_cols[i]->element_truesize);
                    //cout  << bytes <<endl;
                    if(util::compareString(bytes, (char*)&(*stmt->values)[i]->ival)){
                        cout<<"Can't insert duplicate values to primary key column"<<endl;
                        delete bytes;
                        return false;
                    }

                }
            }

        }
    }
    os.close();

    return true;
}

//update check
bool table::updatecheck(hsql::UpdateStatement *stmt) {
    column* col = getColumn((*stmt->updates)[0]->column);
    bool updated = false;
    if(col == NULL){
        cout << "no such column in table to update" <<endl;
        return false;
    }
    if(stmt->where == nullptr){
        //set all column to same value
    }
    else {
        fstream os(getName(), ios::in|ios::out|ios::binary);

        for (int i = 0; i < getRowlength(); i++) {
            if (stmt->where->expr2->type == hsql::kExprLiteralInt || stmt->where->expr->type == hsql::kExprLiteralInt) {
                //select column
                string colName;
                int compareNum;
                if (stmt->where->expr2->type == hsql::kExprLiteralInt) {
                    if (stmt->where->expr->type == hsql::kExprLiteralString) {
                        cout << "cannot do compare between int and char" << endl;
                        return false;
                    }
                    colName = stmt->where->expr->name;
                    compareNum = (int) stmt->where->expr2->ival;
                } else {
                    if (stmt->where->expr2->type == hsql::kExprLiteralString) {
                        cout << "cannot do compare between int and char" << endl;
                        return false;
                    }
                    colName = stmt->where->expr2->name;
                    compareNum = (int) stmt->where->expr->ival;
                }
                column *tempcol = NULL;
                //check if table have this column
                for (auto col: table_cols) {
                    if (colName == col->name) {
                        tempcol = col;
                    }
                }
                if (tempcol == NULL) {
                    cout << "column " << colName << " do not exist in table" << endl;
                    return false;
                }
                //get column value
                os.seekg(i * getrowSize() + tempcol->col_offset);
                char *bytes = new char[tempcol->element_truesize];
                os.read(bytes, tempcol->element_truesize);
                //check =
                if (stmt->where->opChar == '=') {
                    if (tempcol->flag == "CHAR") {
                        cout << "cannot do = for char columns with int value" << endl;
                        delete bytes;
                        return false;
                    }
                    if (*(int *) bytes != compareNum) {
                        delete bytes;
                        continue;
                    }
                }
                //check >
                if (stmt->where->opChar == '>') {
                    if (tempcol->flag == "CHAR") {
                        cout << "cannot do > for char columns with int value" << endl;
                        delete bytes;
                        return false;
                    }
                    if (*(int *) bytes <= compareNum) {
                        delete bytes;
                        continue;
                    }
                }
                //check <
                if (stmt->where->opChar == '<') {
                    if (tempcol->flag == "CHAR") {
                        cout << "cannot do < for char columns with int value" << endl;
                        delete bytes;
                        return false;
                    }
                    if (*(int *) bytes >= compareNum) {
                        delete bytes;
                        continue;
                    }
                }

            }
            if (stmt->where->expr->type == hsql::kExprColumnRef && stmt->where->expr2->type == hsql::kExprColumnRef)
                continue;
            //do char
            if (stmt->where->expr2->type == hsql::kExprLiteralString ||
                stmt->where->expr->type == hsql::kExprLiteralString) {
                //select column
                string colName;
                char *compareChar;
                if (stmt->where->expr2->type == hsql::kExprLiteralString) {
                    colName = stmt->where->expr->name;
                    compareChar = stmt->where->expr2->name;
                } else {
                    colName = stmt->where->expr2->name;
                    compareChar = stmt->where->expr->name;
                }
                column *tempcol = NULL;
                //check if table have this column
                for (auto col: table_cols) {
                    if (colName == col->name) {
                        tempcol = col;
                    }
                }
                if (tempcol == NULL) {
                    cout << "column " << colName << " do not exist in table" << endl;
                    return false;
                }
                //get column value
                os.seekg(i * getrowSize() + tempcol->col_offset);
                char *bytes = new char[tempcol->element_truesize];
                os.read(bytes, tempcol->element_truesize);
                //check =
                if (stmt->where->opChar == '=') {
                    if (!util::compareString(bytes, compareChar)) {
                        delete bytes;
                        continue;
                    }
                }
                    //check >
                else if (stmt->where->opChar == '>') {
                    if (util::compareChar(bytes, compareChar) != 1) {
                        delete bytes;
                        continue;
                    }
                }
                    //check <
                else if (stmt->where->opChar == '<') {
                    if (util::compareChar(bytes, compareChar) != -1) {
                        delete bytes;
                        continue;
                    }
                }

            }
            //do update

            if (col != NULL) {

                if (col->flag == "CHAR") {
                    if (((*stmt->updates)[0]->value)->type == hsql::kExprLiteralString) {

                        updated = true;

                    } else {

                        updated = true;
                    }
                } else {

                    updated = true;

                }
            }

        }
        os.close();
    }
    if(updated)
        return true;
    else{
        cout<<"where condition is wrong"<<endl;
        return false;
    }

}



