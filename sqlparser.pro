TEMPLATE = app
CONFIG += console# c++11
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += main.cpp \
    sqlhelper.cpp \
    SQLParser.cpp \
    SQLParserResult.cpp \
    sql/Expr.cpp \
    sql/statements.cpp \
    parser/bison_parser.cpp \
    parser/flex_lexer.cpp \
    dbms.cpp \
    Schema.cpp \
    Table.cpp \
    TableUtil.cpp

HEADERS += \
    sqlhelper.h \
    SQLParser.h \
    SQLParserResult.h \
    sql/CreateStatement.h \
    sql/DeleteStatement.h \
    sql/DropStatement.h \
    sql/ExecuteStatement.h \
    sql/Expr.h \
    sql/ImportStatement.h \
    sql/InsertStatement.h \
    sql/PrepareStatement.h \
    sql/SelectStatement.h \
    sql/SQLStatement.h \
    sql/statements.h \
    sql/Table.h \
    sql/UpdateStatement.h \
    sql/ShowStatement.h \
    sql/QuitStatement.h \
    parser/bison_parser.h \
    parser/flex_lexer.h \
    parser/parser_typedef.h \
    dbms.h \
    Schema.h \
    Table.h \
    defines.h \
    TableUtil.h

DISTFILES += \
    parser/bison_parser.output
