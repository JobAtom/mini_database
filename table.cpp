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
            column* primary = getPrimaryKey();
            if(table_cols[i]->flag == "CHAR"){

                if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                    const char* str = (*stmt->values)[i]->name ;
                    if( strlen(str) > table_cols[i]->element_truesize ){
                        cout << "Wrong Char size" << endl;
                         return false;
                    }
                    if(util::compareString(table_cols[i]->name, primary->name)){
                        cout << "check primary key values"<<endl;
                        for(int j=0 ; j < getRowlength(); j++){
                            os.seekg(j * rowSize + table_cols[i]->col_offset);
                            char *bytes = new char[table_cols[i]->element_truesize];
                            os.read(bytes, table_cols[i]->element_truesize);
                            if(util::compareString(bytes, str)){
                                cout<<"Can't insert duplicate values to primary key column"<<endl;
                                return false;
                            }

                        }
                    }
                    os.write(str,  table_cols[i]->element_truesize ) ;

                }
                else{
                    const char* str = to_string( (*stmt->values)[i]->ival ).c_str();

                    if( strlen(str) > table_cols[i]->element_truesize ){
                        cout << "Wrong Char size" << endl;
                        return false;
                    }
                    if(util::compareString(table_cols[i]->name, primary->name)){
                        for(int j=0 ; j < getRowlength(); j++){
                            os.seekg(j * rowSize + table_cols[i]->col_offset);
                            char *bytes = new char[table_cols[i]->element_truesize];
                            os.read(bytes, table_cols[i]->element_truesize);
                            if(util::compareString(bytes, str)){
                                cout<<"Can't insert duplicate values to primary key column"<<endl;
                                return false;
                            }

                        }
                    }
                    os.write(str,  table_cols[i]->element_truesize ) ;
                }
            }
            else{
                if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                    cout<<"need a int value instead of a string"<<endl;
                    return false;
                }
                if(util::compareString(table_cols[i]->name, primary->name)){
                    for(int j=0 ; j < getRowlength(); j++){
                        os.seekg(j * rowSize + table_cols[i]->col_offset);
                        char *bytes = new char[table_cols[i]->element_truesize];
                        os.read(bytes, table_cols[i]->element_truesize);
                        cout  << bytes <<endl;
                        if(util::compareString(bytes, (char*)&(*stmt->values)[i]->ival)){
                            cout<<"Can't insert duplicate values to primary key column"<<endl;
                            return false;
                        }

                    }
                }
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
                    cout <<"Column '"<<colName<<"' does not exist"<<endl;
                    return {};
                }else{
                    cols.push_back(make_pair(col->name, col));
                }
            }else if(expr->type == hsql::kExprLiteralInt){
                cols.push_back(make_pair(to_string(expr->ival), (column*)NULL));
            }
        }
    }
    return cols;

}



