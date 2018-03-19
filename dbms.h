#ifndef DBMS_H
#define DBMS_H
#include <fstream>
#include "Schema.h"

using namespace std;
namespace dbms {

enum RUN_MODE{
    SCRIPT = 0,
    INTERACTIVE = 1
};

class DBMS
{
public:
    DBMS();
    ~DBMS();

    void setMode(RUN_MODE mode);
    bool processQuery(const string& query);

    static ostream& log(){
//        static ostream* os;
        return *os;
    };

private:
    RUN_MODE mode;
    static ostream* os;

    Schema* schema;
};

}

#endif // DBMS_H
