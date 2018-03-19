#ifndef __CREATE_STATEMENT_H__
#define __CREATE_STATEMENT_H__

#include "SQLStatement.h"
#include <limits>

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
  struct SelectStatement;

  // Represents definition of a table column
  struct ColumnDefinition {
    enum DataType {
      UNKNOWN,
      TEXT,
      INT,
      DOUBLE,
    };

    ColumnDefinition(char* name, DataType type, int size = std::numeric_limits<int>::max());
    virtual ~ColumnDefinition();

    char* name;
    DataType type;
    int size;
  };


  // Represents SQL Create statements.
  // Example: "CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)"
  struct CreateStatement : SQLStatement {
    enum CreateType {
      kTable,
      kTableFromTbl, // Hyrise file format
      kView
    };

    CreateStatement(CreateType type);
    virtual ~CreateStatement();

    CreateType type;
    bool ifNotExists; // default: false
    char* filePath; // default: NULL
    char* tableName; // default: NULL
    std::vector<ColumnDefinition*>* columns; // default: NULL
    ColumnDefinition* primaryKey;
    std::vector<char*>* viewColumns;
    SelectStatement* select;
  };

} // namespace hsql
#endif
