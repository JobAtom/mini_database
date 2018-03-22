//
// Created by JinAobo on 3/22/18.
//
#include "column.h"
#include <string>


using namespace std;

column::column(){

}
column::column(const string &name, const string &flag, int size){
    this->name = name;
    this->flag = flag;
    if(flag == "INT")
        element_truesize = size;
    else
        element_truesize = size + 1;
    element_size = size;
}
column::~column(){

}

