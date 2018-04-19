cmake_minimum_required(VERSION 3.9)
project(mini_database)

set(CMAKE_CXX_STANDARD 11)

add_executable(mini_database
        parser/bison_parser.cpp
        parser/bison_parser.h
        parser/flex_lexer.cpp
        parser/flex_lexer.h
        parser/parser_typedef.h
        sql/CreateStatement.h
        sql/DeleteStatement.h
        sql/DropStatement.h
        sql/ExecuteStatement.h
        sql/Expr.cpp
        sql/Expr.h
        sql/ImportStatement.h
        sql/InsertStatement.h
        sql/PrepareStatement.h
        sql/QuitStatement.h
        sql/SelectStatement.h
        sql/ShowStatement.h
        sql/SQLStatement.h
        sql/statements.cpp
        sql/statements.h
        sql/Table.h
        sql/UpdateStatement.h
        main.cpp
        sqlhelper.cpp
        sqlhelper.h
        SQLParser.cpp
        SQLParser.h
        SQLParserResult.cpp
        SQLParserResult.h
        table.cpp
        table.h
        column.h
        column.cpp
        util.cpp
        util.h ThreadPool.cpp)
