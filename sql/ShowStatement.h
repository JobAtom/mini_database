#ifndef SHOWSTATEMENT_H
#define SHOWSTATEMENT_H

#include "SQLStatement.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
  // Represents SQL SHOW statements.
  // Example "SHOW TABLE students;"
  struct ShowStatement : SQLStatement {
      enum EntityType {
          kTable,
          kSchema
      };

    ShowStatement(EntityType type);
    virtual ~ShowStatement();

    EntityType type;
    char* tableName;
  };

} // namespace hsql

#endif // SHOWSTATEMENT_H
