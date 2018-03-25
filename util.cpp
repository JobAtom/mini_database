//
// Created by JinAobo on 3/23/18.
//

#include "util.h"


util::util(){

}

table* util::getTable(const string &name, map<string, table*> table_list){

    for(auto tl : table_list){
        cout << tl.first << endl;
        string tl_name = tl.first;
        if(compareString(name, tl_name))
            return tl.second;
    }
    return nullptr;
}
bool util::compareString(const string &str1, const string &str2){
    if(str1.size() != str2.size())
        return false;
    for(int i = 0; i < str1.size(); i++)
        if(tolower(str1[i]) != tolower(str2[i]))
            return false;
    return true;
}
bool util::compareString(const string &str1, const char* str2){
    if(str1.size() != strlen(str2))
        return false;
    for(int i = 0; i < str1.size(); i++)
        if(tolower(str1[i]) != tolower(str2[i]))
            return false;
    return true;
}
bool util::compareString(const char* str1, const char* str2){
    if(strlen(str1) != strlen(str2))
        return false;
    for(int i = 0; i < strlen(str1); i++)
        if(tolower(str1[i]) != tolower(str2[i]))
            return false;
    return true;
}
