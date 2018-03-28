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
    if(stmt->columns == NULL && table_cols.size() < stmt->values->size()){
        cout <<"INSERT has more expressions than target columns"<<endl;
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
    for(int i = 0; i < table_cols.size(); i++){
        if(stmt->columns != NULL){
        //insert value to selected columns
        }
        else{
            if(table_cols[i]->flag == "CHAR"){

                // check char size


                if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                    const char* str = (*stmt->values)[i]->name ;
                    if( strlen(str) > table_cols[i]->element_truesize ){
                        cout << "Wrong Char size" << endl;
                         return false;
                    }
                    os.write(str,  table_cols[i]->element_truesize ) ;

                }
                else{
                    const char* str = to_string( (*stmt->values)[i]->ival ).c_str();

                    if( strlen(str) > table_cols[i]->element_truesize ){
                        cout << "Wrong Char size" << endl;
                        return false;
                    }
                    os.write(str,  table_cols[i]->element_truesize ) ;
                }
            }
            else{
                if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                    cout<<"need a int value instead of a string"<<endl;
                    return false;
                }
                //cout << (*stmt->values)[i]->ival << "Data: " << endl;

                os.write(  (char*)&(*stmt->values)[i]->ival, 8) ;

//                char* bytes = new char[16];
//
//                ifstream ifos(filename, ios::in|ios::binary);
//                ifos.read(bytes, 16);
//
//                if(table_cols[i]->flag == "INT")
//                    cout <<left<<setw(8)<<setfill(' ')<<*(int*)bytes;
//                else{
//                    cout<<left<<setw(table_cols[i]->element_size+2 > 8 ? table_cols[i]->element_size+2 : 8)<<setfill(' ')<<bytes;
//                }
//                delete bytes;

            }

        }
    }
    rowlength++;
    os.close();
    return true;

}


bool table::select(hsql::SelectStatement *stmt){

    ifstream os(filename, ios::in|ios::binary);

    if(!os.is_open()){
        cout <<"Can't open table "<< stmt->fromTable->name  <<endl;
        return false;
    }
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
                    cout <<"Column '"<<colName<<"' does not exist"<<endl;
                    return false;
                }else{
                    cols.push_back(make_pair(col->name, col));
                }
            }else if(expr->type == hsql::kExprLiteralInt){
                cols.push_back(make_pair(to_string(expr->ival), (column*)NULL));
            }
        }
    }


    // header
    for(auto it : cols){
        column* col = it.second;
        if(col == NULL || col->flag == "INT")
           cout <<left<<setw(8)<<setfill(' ')<<it.first;
        else if(col->flag == "CHAR")
            cout <<left<<setw(col->element_size +2 > 8 ? col->element_size+2 : 8)<<setfill(' ')<<it.first;
    }
    cout << endl;

    for(int i = 0; i < rowlength; i++){

        //where condition
        if(stmt->whereClause != NULL){
            if(stmt->whereClause->type != hsql::kExprOperator ||
               stmt->whereClause->opType != hsql::Expr::SIMPLE_OP||
               (stmt->whereClause->opChar != '=' && stmt->whereClause->opChar != '>'&&
                stmt->whereClause->opChar != '<')){
                cout <<"Invalide where clause"<<endl;
                return false;
            }
            //deal with int
            if(stmt->whereClause->expr2->type == hsql::kExprLiteralInt||stmt->whereClause->expr->type == hsql::kExprLiteralInt){
                //select column
                string colName;
                int compareNum;
                if(stmt->whereClause->expr2->type == hsql::kExprLiteralInt) {
                    if(stmt->whereClause->expr->type == hsql::kExprLiteralString){
                        cout << "cannot do compare between int and char" << endl;
                        return false;
                    }
                    colName = stmt->whereClause->expr->name;
                    compareNum = (int) stmt->whereClause->expr2->ival;
                }
                else {
                    if(stmt->whereClause->expr2->type == hsql::kExprLiteralString){
                        cout << "cannot do compare between int and char" << endl;
                        return false;
                    }
                    colName = stmt->whereClause->expr2->name;
                    compareNum = (int) stmt->whereClause->expr->ival;
                }
                column* tempcol = NULL;
                //check if table have this column
                for (auto col: table_cols){
                    if(colName == col->name){
                        tempcol = col;
                    }
                }
                if(tempcol == NULL){
                    cout << "column "<< colName << " do not exist in table" <<endl;
                    return false;
                }
                //get column value
                os.seekg(i * rowSize + tempcol->col_offset);
                char *bytes = new char[tempcol->element_truesize];
                os.read(bytes, tempcol->element_truesize);
                //check =
                if(stmt->whereClause->opChar == '=') {
                    if(tempcol->flag == "CHAR"){
                        cout << "cannot do = for char columns with int value" <<endl;
                        delete bytes;
                        return false;
                    }
                    if (*(int *) bytes != compareNum) {
                        delete bytes;
                        continue;
                    }
                }
                //check >
                if(stmt->whereClause->opChar == '>'){
                    if(tempcol->flag == "CHAR"){
                        cout << "cannot do > for char columns with int value" <<endl;
                        delete bytes;
                        return false;
                    }
                    if(*(int *)bytes <= compareNum){
                        delete bytes;
                        continue;
                    }
                }
                //check <
                if(stmt->whereClause->opChar == '<'){
                    if(tempcol->flag == "CHAR"){
                        cout << "cannot do < for char columns with int value" <<endl;
                        delete bytes;
                        return false;
                    }
                    if(*(int *)bytes >= compareNum){
                        delete bytes;
                        continue;
                    }
                }

            }
            //do char
            if(stmt->whereClause->expr2->type == hsql::kExprLiteralString||stmt->whereClause->expr->type == hsql::kExprLiteralString){
                //select column
                string colName;
                char* compareChar;
                if(stmt->whereClause->expr2->type == hsql::kExprLiteralString) {
                    colName = stmt->whereClause->expr->name;
                    compareChar = stmt->whereClause->expr2->name;
                }
                else {
                    colName = stmt->whereClause->expr2->name;
                    compareChar = stmt->whereClause->expr->name;
                }
                column* tempcol = NULL;
                //check if table have this column
                for (auto col: table_cols){
                    if(colName == col->name){
                        tempcol = col;
                    }
                }
                if(tempcol == NULL){
                    cout << "column "<< colName << " do not exist in table" <<endl;
                    return false;
                }
                //get column value
                os.seekg(i * rowSize + tempcol->col_offset);
                char *bytes = new char[tempcol->element_truesize];
                os.read(bytes, tempcol->element_truesize);
                //check =
                if(stmt->whereClause->opChar == '=') {
                    if (!util::compareString(bytes ,compareChar)) {
                        delete bytes;
                        continue;
                    }
                }
                //check >
                else if(stmt->whereClause->opChar == '>'){
                    if(util::compareChar(bytes, compareChar) != 1){
                        delete bytes;
                        continue;
                    }
                }
                //check <
                else if(stmt->whereClause->opChar == '<'){
                    if(util::compareChar(bytes, compareChar) != -1){
                        delete bytes;
                        continue;
                    }
                }

            }

        }
        for(auto it:cols) {
            column *col = it.second;

            if (col != NULL) {
                //cout << "rowSize : " << rowSize << endl;


                os.seekg(i * rowSize + col->col_offset); //i*element_true_size + col->col_offset
                //cout << "position : " << i * rowSize + col->col_offset<< endl;
                //TableUtil::printColValue(os, col);

                char *bytes = new char[col->element_truesize];
                //cout << "bytes: " << bytes << " -- true size: " << col->element_truesize << endl;
                os.read(bytes, col->element_truesize);


                if ( col->flag == "INT")
                    cout << left << setw(8) << setfill(' ') << *(int *) bytes;
                else {
                    cout << left << setw(col->element_size + 2 > 8 ? col->element_size + 2 : 8)
                         << setfill(' ') << bytes;
                }
                delete bytes;

            } else {
                cout << left << setw(8) << setfill(' ') << it.first;
            }
        }
        cout << endl;
    }
    os.close();

}



