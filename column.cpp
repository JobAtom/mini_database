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

void column::fromString(const string &str){
    int idx = str.find(":");
    name = str.substr(0, idx);
    string tmp = str.substr(idx+1);
    if(tmp == "INT"){
        flag = "INT";
        element_size = 8;
        element_truesize = 8;
    }
    else if(tmp.substr(0, 4) == "CHAR"){
        flag = "CHAR";
        idx = tmp.find(")");
        tmp  = tmp.substr(5, idx-5);
        element_size = stoi(tmp);
        element_truesize = element_size + 1;
    }
}

column::~column(){

}


