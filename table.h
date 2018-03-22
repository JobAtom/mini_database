#ifndef TABLE_H
#define TABLE_H
#include <vector>
#include <string>

using namespace std;


class table{
public:
    table();
    table(const string& name);
    virtual ~table();



private:
    string filename;


};

#endif