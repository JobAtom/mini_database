#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include "table.h"
#include "util.h"
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
        cout << "save cols"<<endl;
        if(stmt->columns != NULL){
        //insert value to selected columns
        }
        else{
            if((*stmt->values)[i]->type == hsql::kExprLiteralString){
                os << (*stmt->values)[i]->name;
            }
            else{
                os << (*stmt->values)[i]->ival;
            }
            rowlength++;
        }

    }
    os.close();
    return true;

}

