#include "minisql.h"
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>
#include <cctype>

using namespace std;

// ==================== 辅助函数声明 ====================
vector<string> split(const string& str, char delimiter);
string trim(const string& str);
vector<Column> parseColumnDefinitions(const string& columns_str);
shared_ptr<LogicExpression> parseWhereClause(const string& where_str, const shared_ptr<Table>& table);
JoinCondition parseJoinCondition(const string& join_str);
void displayResults(const vector<Row>& results, const vector<Column>& columns);
void handleCreateTable(MiniSQL& db, const string& input);
void handleInsert(MiniSQL& db, const string& input);
void handleSimpleSelect(MiniSQL& db, const string& input);
void handleJoinSelect(MiniSQL& db, const string& input, bool has_save_as, const string& save_table_name);
void showHelp();

// ==================== 辅助函数实现 ====================
vector<string> split(const string& str, char delimiter) {
    vector<string> tokens;
    string token;
    istringstream tokenStream(str);
    while (getline(tokenStream, token, delimiter)) {
        token = trim(token);
        if (!token.empty()) {
            tokens.push_back(token);
        }
    }
    return tokens;
}

string trim(const string& str) {
    if (str.empty()) return str;
    
    string result = str;
    
    size_t start = result.find_first_not_of(" \t\n\r");
    if (start != string::npos) {
        result = result.substr(start);
    } else {
        return "";
    }
    
    size_t end = result.find_last_not_of(" \t\n\r");
    if (end != string::npos) {
        result = result.substr(0, end + 1);
    }
    
    if (!result.empty() && result.back() == ';') {
        result.pop_back();
    }
    
    return result;
}

vector<Column> parseColumnDefinitions(const string& columns_str) {
    vector<Column> columns;
    vector<string> column_tokens = split(columns_str, ',');
    
    for (const auto& column_def : column_tokens) {
        if (column_def.empty()) continue;
        
        size_t last_space = column_def.find_last_of(' ');
        if (last_space == string::npos) {
            cerr << "Error: Invalid column definition: " << column_def << endl;
            continue;
        }
        
        string col_name = trim(column_def.substr(0, last_space));
        string col_type_str = column_def.substr(last_space + 1);
        transform(col_type_str.begin(), col_type_str.end(), col_type_str.begin(), ::toupper);
        
        Column col;
        col.name = col_name;
        
        if (col_type_str.find("INT") == 0) {
            col.type = "INT";
        } else if (col_type_str.find("DOUBLE") == 0) {
            col.type = "DOUBLE";
        } else if (col_type_str.find("VARCHAR") == 0) {
            col.type = "VARCHAR";
            size_t open_paren = col_type_str.find('(');
            if (open_paren != string::npos) {
                size_t close_paren = col_type_str.find(')', open_paren);
                if (close_paren != string::npos) {
                    string length_str = col_type_str.substr(open_paren + 1, close_paren - open_paren - 1);
                    try {
                        col.varchar_length = stoul(length_str);
                    } catch (...) {
                        col.varchar_length = 255;
                    }
                }
            } else {
                col.varchar_length = 255;
            }
        } else {
            cerr << "Warning: Unrecognized type '" << col_type_str << "', defaulting to VARCHAR" << endl;
            col.type = "VARCHAR";
            col.varchar_length = 255;
        }
        
        columns.push_back(col);
    }
    
    return columns;
}

shared_ptr<LogicExpression> parseWhereClause(const string& where_str, const shared_ptr<Table>& table) {
    if (!table || where_str.empty()) {
        return nullptr;
    }
    
    return WhereParser::parse(trim(where_str), table->columns());
}

JoinCondition parseJoinCondition(const string& join_str) {
    JoinCondition condition;
    string str = trim(join_str);
    
    size_t dot1 = str.find('.');
    size_t equal_pos = str.find('=');
    size_t dot2 = str.find('.', equal_pos);
    
    if (dot1 != string::npos && equal_pos != string::npos && dot2 != string::npos) {
        condition.left_table = trim(str.substr(0, dot1));
        condition.left_column = trim(str.substr(dot1 + 1, equal_pos - dot1 - 1));
        condition.right_table = trim(str.substr(equal_pos + 1, dot2 - equal_pos - 1));
        condition.right_column = trim(str.substr(dot2 + 1));
        condition.op = CompareOp::EQUAL;
    }
    
    return condition;
}

void displayResults(const vector<Row>& results, const vector<Column>& columns) {
    if (results.empty()) {
        cout << "No records found" << endl;
        return;
    }
    
    cout << "\nQuery results (" << results.size() << " records):" << endl;
    
    if (!columns.empty()) {
        for (size_t i = 0; i < columns.size(); ++i) {
            cout << columns[i].name;
            if (i < columns.size() - 1) cout << "\t";
        }
        cout << endl;
        cout << string(columns.size() * 15, '-') << endl;
    } else {
        for (size_t i = 0; i < results[0].size(); ++i) {
            cout << "Column " << (i + 1);
            if (i < results[0].size() - 1) cout << "\t";
        }
        cout << endl;
        cout << string(results[0].size() * 15, '-') << endl;
    }
    
    for (const auto& row : results) {
        for (size_t i = 0; i < row.size(); ++i) {
            visit([](auto&& arg) {
                cout << arg;
            }, row[i]);
            if (i < row.size() - 1) cout << "\t";
        }
        cout << endl;
    }
}

// ==================== 命令处理函数 ====================
void handleCreateTable(MiniSQL& db, const string& input) {
    size_t open_paren = input.find('(');
    size_t close_paren = input.rfind(')');
    
    if (open_paren == string::npos || close_paren == string::npos || close_paren <= open_paren) {
        cout << "Error: Invalid CREATE TABLE syntax." << endl;
        cout << "Correct format: CREATE TABLE table_name (col1 type, col2 type, ...)" << endl;
        return;
    }
    
    string table_name = trim(input.substr(12, open_paren - 12));
    if (table_name.empty()) {
        cout << "Error: Table name cannot be empty" << endl;
        return;
    }
    
    string columns_str = input.substr(open_paren + 1, close_paren - open_paren - 1);
    vector<Column> columns = parseColumnDefinitions(columns_str);
    
    if (columns.empty()) {
        cout << "Error: No valid column definitions found" << endl;
        return;
    }
    
    db.createTable(table_name, columns, table_name + ".csv");
    
    cout << "Table '" << table_name << "' created successfully!" << endl;
    cout << "Columns: ";
    for (size_t i = 0; i < columns.size(); ++i) {
        cout << columns[i].name << " " << columns[i].type;
        if (columns[i].type == "VARCHAR" && columns[i].varchar_length > 0) {
            cout << "(" << columns[i].varchar_length << ")";
        }
        if (i < columns.size() - 1) cout << ", ";
    }
    cout << endl;
}

void handleInsert(MiniSQL& db, const string& input) {
    size_t values_pos = input.find("VALUES");
    if (values_pos == string::npos) {
        cout << "Syntax error: INSERT INTO <table_name> VALUES (...)" << endl;
        return;
    }
    
    string table_name = trim(input.substr(11, values_pos - 11));
    if (table_name.empty()) {
        cout << "Error: Table name cannot be empty" << endl;
        return;
    }
    
    string values_str = trim(input.substr(values_pos + 6));
    if (values_str.front() == '(' && values_str.back() == ')') {
        values_str = values_str.substr(1, values_str.length() - 2);
    }
    
    vector<string> value_tokens = split(values_str, ',');
    vector<Value> row_values;
    
    for (const auto& val : value_tokens) {
        string cleaned_val = val;
        if (cleaned_val.front() == '\'' && cleaned_val.back() == '\'') {
            cleaned_val = cleaned_val.substr(1, cleaned_val.length() - 2);
        }
        
        try {
            row_values.push_back(stoi(cleaned_val));
        } catch (...) {
            try {
                row_values.push_back(stod(cleaned_val));
            } catch (...) {
                row_values.push_back(cleaned_val);
            }
        }
    }
    
    Row row(row_values);
    bool success = db.insert(table_name, row);
    if (success) {
        cout << "Data inserted successfully!" << endl;
    } else {
        cout << "Insert failed: Table does not exist or column count mismatch" << endl;
    }
}

void handleSimpleSelect(MiniSQL& db, const string& input) {
    string upper_input = input;
    transform(upper_input.begin(), upper_input.end(), upper_input.begin(), ::toupper);
    
    size_t from_pos = upper_input.find("FROM");
    if (from_pos == string::npos) {
        cout << "Syntax error: SELECT * FROM <table_name>" << endl;
        return;
    }
    
    string select_part = trim(input.substr(0, from_pos));
    if (select_part != "SELECT *") {
        cout << "Only SELECT * FROM is currently supported" << endl;
        return;
    }
    
    string table_name;
    shared_ptr<LogicExpression> where_clause = nullptr;
    size_t where_pos = upper_input.find("WHERE");
    
    if (where_pos != string::npos) {
        table_name = trim(input.substr(from_pos + 4, where_pos - from_pos - 4));
        string where_str = trim(input.substr(where_pos + 5));
        auto table = db.getTable(table_name);
        where_clause = parseWhereClause(where_str, table);
    } else {
        table_name = trim(input.substr(from_pos + 4));
    }
    
    if (table_name.empty()) {
        cout << "Error: Table name cannot be empty" << endl;
        return;
    }
    
    vector<Row> results = db.select(table_name, {"*"}, where_clause);
    
    auto table = db.getTable(table_name);
    if (table) {
        displayResults(results, table->columns());
    } else {
        displayResults(results, {});
    }
}

void handleJoinSelect(MiniSQL& db, const string& input, bool has_save_as, const string& save_table_name) {
    string original_input = input;
    string upper_input = input;
    transform(upper_input.begin(), upper_input.end(), upper_input.begin(), ::toupper);
    
    size_t from_pos = upper_input.find("FROM");
    size_t join_pos = upper_input.find("JOIN");
    size_t on_pos = upper_input.find("ON");
    size_t where_pos = upper_input.find("WHERE");
    
    if (from_pos == string::npos || join_pos == string::npos || on_pos == string::npos) {
        cout << "Syntax error: SELECT * FROM <table1> JOIN <table2> ON <condition>" << endl;
        return;
    }
    
    string table1 = trim(original_input.substr(from_pos + 4, join_pos - from_pos - 4));
    string table2;
    
    if (where_pos != string::npos) {
        table2 = trim(original_input.substr(join_pos + 4, where_pos - join_pos - 4));
    } else {
        table2 = trim(original_input.substr(join_pos + 4, on_pos - join_pos - 4));
    }
    
    string join_condition_str;
    if (where_pos != string::npos) {
        join_condition_str = trim(original_input.substr(on_pos + 2, where_pos - on_pos - 2));
    } else {
        join_condition_str = trim(original_input.substr(on_pos + 2));
    }
    
    JoinCondition join_condition = parseJoinCondition(join_condition_str);
    
    shared_ptr<LogicExpression> where_clause = nullptr;
    if (where_pos != string::npos) {
        string where_str = trim(original_input.substr(where_pos + 5));
        auto table = db.getTable(table1);
        where_clause = parseWhereClause(where_str, table);
    }
    
    if (has_save_as) {
        bool success = db.saveJoinAsTable(save_table_name, table1, table2, join_condition, where_clause);
        if (success) {
            cout << "JOIN results saved as table: '" << save_table_name << "'" << endl;
        } else {
            cout << "Failed to save JOIN results" << endl;
        }
    } else {
        vector<Row> results = db.join(table1, table2, {"*"}, JoinType::INNER_JOIN, join_condition, where_clause);
        
        auto left_table = db.getTable(table1);
        auto right_table = db.getTable(table2);
        
        if (left_table && right_table) {
            vector<Column> merged_columns;
            
            for (const auto& col : left_table->columns()) {
                Column new_col = col;
                new_col.name = table1 + "_" + col.name;
                merged_columns.push_back(new_col);
            }
            
            for (const auto& col : right_table->columns()) {
                Column new_col = col;
                new_col.name = table2 + "_" + col.name;
                merged_columns.push_back(new_col);
            }
            
            displayResults(results, merged_columns);
        } else {
            displayResults(results, {});
        }
    }
}

void showHelp() {
    cout << "\nAvailable commands:" << endl;
    cout << "  CREATE TABLE <table_name> (<column_definitions>)" << endl;
    cout << "    Example: CREATE TABLE employees (id INT, name VARCHAR(50), age INT)" << endl;
    cout << endl;
    cout << "  INSERT INTO <table_name> VALUES (...)" << endl;
    cout << "    Example: INSERT INTO employees VALUES (1, 'Alice', 28)" << endl;
    cout << endl;
    cout << "  SELECT * FROM <table_name> [WHERE condition]" << endl;
    cout << "    Example: SELECT * FROM employees WHERE age > 25" << endl;
    cout << endl;
    cout << "  SELECT * FROM <table1> JOIN <table2> ON <condition> [SAVE AS <new_table>]" << endl;
    cout << "    Example: SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id" << endl;
    cout << "    Example with SAVE: SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id SAVE AS emp_dept" << endl;
    cout << endl;
    cout << "  DROP TABLE <table_name> - Delete a table" << endl;
    cout << "  SHOW TABLES - List all tables" << endl;
    cout << "  EXIT - Exit the program" << endl;
    cout << "  HELP - Show this help message" << endl;
}

// ==================== 主函数 ====================
int main() {
    MiniSQL db;
    string input;
    
    cout << "====== Welcome to MiniSQL Database System ======" << endl;
    cout << "Type 'HELP' for available commands" << endl;
    cout << "Type 'EXIT' to quit" << endl;
    
    while (true) {
        cout << "\nminisql> ";
        getline(cin, input);
        
        if (input.empty()) continue;
        
        input = trim(input);
        string upper_input = input;
        transform(upper_input.begin(), upper_input.end(), upper_input.begin(), ::toupper);
        
        if (upper_input == "EXIT") {
            cout << "Saving all tables to CSV..." << endl;
            db.saveAllTables();
            cout << "Thank you for using MiniSQL!" << endl;
            break;
        }
        
        if (upper_input == "HELP") {
            showHelp();
            continue;
        }
        
        if (upper_input == "SHOW TABLES") {
            cout << "Tables in database:" << endl;
            cout << "-------------------" << endl;
            auto tables = db.listTables();
            if (tables.empty()) {
                cout << "No tables found" << endl;
            } else {
                for (const auto& table_name : tables) {
                    cout << "- " << table_name << endl;
                }
            }
            continue;
        }
        
        if (upper_input.find("DROP TABLE") == 0) {
            string table_name = trim(input.substr(10));
            if (table_name.empty()) {
                cout << "Error: Table name cannot be empty" << endl;
                continue;
            }
            db.dropTable(table_name);
            continue;
        }
        
        if (upper_input.find("CREATE TABLE") == 0) {
            try {
                handleCreateTable(db, input);
            } catch (const exception& e) {
                cout << "Error creating table: " << e.what() << endl;
            }
            continue;
        }
        
        if (upper_input.find("INSERT INTO") == 0) {
            try {
                handleInsert(db, input);
            } catch (const exception& e) {
                cout << "Insert error: " << e.what() << endl;
            }
            continue;
        }
        
        if (upper_input.find("SELECT") == 0) {
            try {
                bool has_save_as = false;
                string save_table_name = "";
                size_t save_as_pos = upper_input.find("SAVE AS");
                
                if (save_as_pos != string::npos) {
                    has_save_as = true;
                    save_table_name = trim(input.substr(save_as_pos + 7));
                    input = trim(input.substr(0, save_as_pos));
                }
                
                size_t join_pos = upper_input.find("JOIN");
                
                if (join_pos != string::npos) {
                    handleJoinSelect(db, input, has_save_as, save_table_name);
                } else {
                    handleSimpleSelect(db, input);
                }
            } catch (const exception& e) {
                cout << "Query error: " << e.what() << endl;
            }
            continue;
        }
        
        cout << "Unknown command. Type HELP for available commands" << endl;
    }
    
    return 0;
}