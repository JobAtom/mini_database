#include <iostream>
#include <cstring>
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

