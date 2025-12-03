// File: main.cpp - CLI交互
#include "minisql.h"
#include <iostream>
#include <string>

int main() {
    MiniSQL db;
    std::string input;
    
    std::cout << "=== MiniSQL 数据库系统 ===" << std::endl;
    std::cout << "输入 'EXIT;' 退出" << std::endl;
    std::cout << "输入 'HELP;' 查看帮助" << std::endl;
    
    while (true) {
        std::cout << "\nminisql> ";
        std::getline(std::cin, input);
        
        if (input == "EXIT;") {
            break;
        } else if (input == "HELP;") {
            std::cout << "支持的SQL命令:" << std::endl;
            std::cout << "  CREATE TABLE <table_name> (<column_defs>);" << std::endl;
            std::cout << "  SELECT <columns> FROM <table_name> [WHERE ...];" << std::endl;
            std::cout << "  INSERT INTO <table_name> VALUES (...);" << std::endl;
            continue;
        }
        
        if (input.back() == ';') {
            input.pop_back();
        }
        
        bool success = db.execute(input);
        if (success) {
            std::cout << "执行成功" << std::endl;
        } else {
            std::cout << "执行失败或语法错误" << std::endl;
        }
    }
    
    std::cout << "感谢使用MiniSQL！" << std::endl;
    return 0;
}