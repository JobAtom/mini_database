//
// Created by JinAobo on 3/22/18.
//

#ifndef MINI_DATABASE_COLUMN_H
#define MINI_DATABASE_COLUMN_H
#include <string>

using namespace std;

class column{
public:
    column();
    column(const string &name, const string &flag, int size);
    void fromString(const string &str);
    ~column();

    int col_offset = 0;
    int element_size;
    int element_truesize;

    string name;
    string flag;


};

#endif //MINI_DATABASE_COLUMN_H
