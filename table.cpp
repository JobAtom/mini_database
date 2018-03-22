#include <iostream>
#include <cstring>
#include <vector>
#include "table.h"
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
column* table::getColumn(string name){

}


