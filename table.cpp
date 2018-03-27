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
        cout << "save cols"<<endl;
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


    // header
    for(auto it : cols){
        column* col = it.second;
        if(col == NULL || col->flag == "INT")
           cout <<left<<setw(8)<<setfill(' ')<<it.first;
        else if(col->flag == "CHAR")
            cout <<left<<setw(col->element_size +2 > 8 ? col->element_size+2 : 8)<<setfill(' ')<<it.first;
    }
    //
    //cout << rowlength << endl;
    for(int i = 0; i < rowlength; i++){


        for(auto it:cols) {
            column *col = it.second;

            if (col != NULL) {
                cout << "rowSize : " << rowSize << endl;


                os.seekg(i * rowSize + col->col_offset); //i*element_true_size + col->col_offset
                cout << "position : " << i * rowSize + col->col_offset<< endl;
                //TableUtil::printColValue(os, col);

                char *bytes = new char[col->element_truesize];
                cout << "bytes: " << bytes << " -- true size: " << col->element_truesize << endl;
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
            cout << endl;
        }
    }
    os.close();

}



