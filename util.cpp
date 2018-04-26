//
// Created by JinAobo on 3/23/18.
//

#include "util.h"


util::util(){

}

table* util::getTable(const string &name, map<string, table*> table_list){
    for(auto tl : table_list){
        //cout << tl.first << endl;
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
int util::compareChar(const char* str1, const char* str2){

    for(int i = 0 ; i < min(strlen(str1), strlen(str2)); i++){
        if(str1[i] < str2[i])
            return -1;
        else if (str1[i] > str2[i])
            return 1;
    }
    if(strlen(str2) > strlen(str1))
        return -1;
    else if(strlen(str2) == strlen(str1))
        return 0;
    return 1;
}
bool util::PrintRecords(hsql::SelectStatement *stmt, vector<pair<string, column*>> cols, table* t, int &selectvalue){
    ifstream os(t->getName(), ios::in|ios::binary);

    if(!os.is_open()){
        cout <<"Can't open table "<< stmt->fromTable->name  <<endl;
        return false;
    }
    if(cols.empty()){
        return false;
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

    vector<int> printedrecords;
    //print records
    for(int i = 0; i < t->getRowlength(); i++){

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
                for (auto col: t->table_cols){
                    if(colName == col->name){
                        tempcol = col;
                    }
                }
                if(tempcol == NULL){
                    cout << "column "<< colName << " do not exist in table" <<endl;
                    return false;
                }
                //get column value
                os.seekg(i * t->getrowSize() + tempcol->col_offset);
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
            if(stmt->whereClause->expr->type == hsql::kExprColumnRef&&stmt->whereClause->expr2->type == hsql::kExprColumnRef)
                continue;
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
                for (auto col: t->table_cols){
                    if(colName == col->name){
                        tempcol = col;
                    }
                }
                if(tempcol == NULL){
                    cout << "column "<< colName << " do not exist in table" <<endl;
                    return false;
                }
                //get column value
                os.seekg(i * t->getrowSize() + tempcol->col_offset);
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
        //check duplicates
        bool duplicates = false;
        for(int j = 0; j<i; j++){
            if(find(printedrecords.begin(), printedrecords.end(), j)== printedrecords.end())
                continue;
            int count_duplicates = 0;
            int countcol = 0;
            for(auto it:cols){
                column* col = it.second;
                if (col!=NULL){
                    os.seekg(j * t->getrowSize() + col->col_offset);
                    char* prebytes = new char[col->element_truesize];
                    os.read(prebytes, col->element_truesize);

                    os.seekg(i * t->getrowSize() + col->col_offset);
                    char* curbytes = new char[col->element_truesize];
                    os.read(curbytes, col->element_truesize);
                    if(util::compareString(prebytes, curbytes))
                        count_duplicates += 1;
                    countcol += 1;
                    delete prebytes;
                    delete curbytes;
                }

            }
            if(count_duplicates == countcol)
                duplicates = true;
        }
        if(duplicates)
            continue;
        //do print

        for(auto it:cols) {
            column *col = it.second;
            printedrecords.push_back(i);

            if (col != NULL) {

                os.seekg(i * t->getrowSize() + col->col_offset);
                char *bytes = new char[col->element_truesize];
                //cout << "bytes: " << bytes << " -- true size: " << col->element_truesize << endl;
                os.read(bytes, col->element_truesize);


                if ( col->flag == "INT"){
                    cout << left << setw(8) << setfill(' ') << *(int *) bytes;
                    selectvalue = *(int *) bytes;

                }

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
    return true;
}
bool util::PrintJoinRecords(hsql::SelectStatement *stmt, vector<pair<string, column *>> colsleft,
                            vector<pair<string, column *>> colsright, table *tleft, table *tright) {
    ifstream osleft(tleft->getName(), ios::in|ios::binary);
    ifstream osright(tright->getName(), ios::in|ios::binary);

    if(!osleft.is_open() or !osright.is_open()){
        cout <<"Can't open table "<< stmt->fromTable->name  <<endl;
        return false;
    }
    // header
    for(auto it : colsleft){
        column* col = it.second;
        if(col == NULL || col->flag == "INT")
            cout <<left<<setw(8)<<setfill(' ')<<it.first;
        else if(col->flag == "CHAR")
            cout <<left<<setw(col->element_size +2 > 8 ? col->element_size+2 : 8)<<setfill(' ')<<it.first;
    }
    for(auto it : colsright){
        column* col = it.second;
        if(col == NULL || col->flag == "INT")
            cout <<left<<setw(8)<<setfill(' ')<<it.first;
        else if(col->flag == "CHAR")
            cout <<left<<setw(col->element_size +2 > 8 ? col->element_size+2 : 8)<<setfill(' ')<<it.first;
    }
    cout << endl;
    //print left records
    for(int i = 0; i < tleft->getRowlength(); i++) {
        for (int j = 0; j < tright->getRowlength(); j++) {
            //check left condition
            if(stmt->fromTable->join->condition != NULL){
                if (stmt->fromTable->join->condition->type != hsql::kExprOperator ||
                    stmt->fromTable->join->condition->opType != hsql::Expr::SIMPLE_OP ||
                    stmt->fromTable->join->condition->opChar != '=') {
                    cout << "Invalide join condition" << endl;
                    return false;
                }
                if (stmt->fromTable->join->condition->expr->table != NULL && stmt->fromTable->join->condition->expr2->table != NULL){
                    char* left_tablename = stmt->fromTable->join->condition->expr->table;
                    char* right_tablename = stmt->fromTable->join->condition->expr2->table;
                    char* left_colname = stmt->fromTable->join->condition->expr->name;
                    char* right_colname = stmt->fromTable->join->condition->expr2->name;
                    if (!util::compareString(left_tablename, stmt->fromTable->join->left->name)){
                        cout << "table " << left_tablename << " do not exist" << endl;
                        return false;
                    }
                    else if (!util::compareString(right_tablename, stmt->fromTable->join->right->name)){
                        cout << "table " << right_tablename << " do not exist" << endl;
                        return false;
                    }
                    if( tleft->getColumn(left_colname) == NULL || tright->getColumn(right_colname) == NULL){
                        cout << "table do not exist those columns" << endl;
                        return false;
                    }
                    //check column value
                    column* colleft = tleft->getColumn(left_colname);
                    column* colright = tright->getColumn(right_colname);
                    osleft.seekg(i * tleft->getrowSize() + colleft->col_offset);
                    char *bytesleft = new char[colleft->element_truesize];
                    osleft.read(bytesleft, colleft->element_truesize);

                    osright.seekg(j * tright->getrowSize() + colright->col_offset);
                    char *bytesright = new char[colright->element_truesize];
                    osright.read(bytesright, colright->element_truesize);
                    if(!util::compareString(bytesleft, bytesright))
                        continue;

                }

            }

            //printleft
            for (auto it:colsleft) {
                column *col = it.second;

                if (col != NULL) {
                    //cout << "rowSize : " << rowSize << endl;


                    osleft.seekg(i * tleft->getrowSize() + col->col_offset); //i*element_true_size + col->col_offset
                    //cout << "position : " << i * rowSize + col->col_offset<< endl;
                    //TableUtil::printColValue(os, col);

                    char *bytes = new char[col->element_truesize];
                    //cout << "bytes: " << bytes << " -- true size: " << col->element_truesize << endl;
                    osleft.read(bytes, col->element_truesize);


                    if (col->flag == "INT")
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


            //printright
            for (auto it:colsright) {
                column *col = it.second;

                if (col != NULL) {
                    //cout << "rowSize : " << rowSize << endl;


                    osright.seekg(j * tright->getrowSize() + col->col_offset); //i*element_true_size + col->col_offset
                    //cout << "position : " << i * rowSize + col->col_offset<< endl;
                    //TableUtil::printColValue(os, col);

                    char *bytes = new char[col->element_truesize];
                    //cout << "bytes: " << bytes << " -- true size: " << col->element_truesize << endl;
                    osright.read(bytes, col->element_truesize);


                    if (col->flag == "INT")
                        cout << setw(8) << setfill(' ') << *(int *) bytes;
                    else {
                        cout << setw(col->element_size + 2 > 8 ? col->element_size + 2 : 8)
                             << setfill(' ') << bytes;
                    }
                    delete bytes;

                } else {
                    cout << setw(8) << setfill(' ') << it.first;
                }
            }
            cout << endl;

        }
    }
    osright.close();
    osleft.close();
}

