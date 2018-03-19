#ifndef QUITSTATEMENT_H
#define QUITSTATEMENT_H

#include "SQLStatement.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
  // quit.
  // Example "QUIT;"
  struct QuitStatement : SQLStatement {

    QuitStatement();
    virtual ~QuitStatement();
  };

} // namespace hsql

#endif // QUITSTATEMENT_H
