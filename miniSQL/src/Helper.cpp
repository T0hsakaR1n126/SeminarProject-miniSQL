#include "../include/Helper.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <regex>

using namespace std;

//Realization of helper functions in helper.h

//Part I.Realization of query parser related helper functions.
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
    
    return result;
}

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

char toUpperChar(char c) {
    if (c >= 'a' && c <= 'z') return c - 'a' + 'A';
    return c;
}

size_t findOuterOperator(const string& expr, const string& op) {
    int paren_depth = 0;
    
    for (size_t i = 0; i < expr.size(); ++i) {
        if (expr[i] == '(') paren_depth++;
        else if (expr[i] == ')') paren_depth--;
        
        if (paren_depth == 0 && i + op.size() <= expr.size()) {
            bool match = true;
            for (size_t j = 0; j < op.size(); ++j) {
                if (toUpperChar(expr[i + j]) != toUpperChar(op[j])) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                bool left_ok = (i == 0 || isspace(expr[i-1]) || expr[i-1] == '(');
                bool right_ok = (i + op.size() == expr.size() || 
                               isspace(expr[i + op.size()]) || expr[i + op.size()] == ')');
                
                if (left_ok && right_ok) {
                    return i;
                }
            }
        }
    }
    
    return string::npos;
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

shared_ptr<LogicExpression> parseJoinWhereClause(
    const string& where_str, 
    const shared_ptr<Table>& left_table,
    const shared_ptr<Table>& right_table) {
    
    if (!left_table || !right_table || where_str.empty()) {
        return nullptr;
    }
    
    vector<Column> all_columns;
    all_columns.insert(all_columns.end(), left_table->columns().begin(), left_table->columns().end());
    all_columns.insert(all_columns.end(), right_table->columns().begin(), right_table->columns().end());
    
    return WhereParser::parse(where_str, all_columns);
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

unordered_map<string, Value> parseUpdateSet(const string& set_clause, const shared_ptr<Table>& table) {
    unordered_map<string, Value> updates;
    
    if (!table) return updates;
    
    vector<string> assignments = split(set_clause, ',');
    
    for (const auto& assignment : assignments) {
        size_t equal_pos = assignment.find('=');
        if (equal_pos == string::npos) {
            cout << "Error: Invalid assignment: " << assignment << endl;
            continue;
        }
        
        string col_name = trim(assignment.substr(0, equal_pos));
        string value_str = trim(assignment.substr(equal_pos + 1));
        
        string col_type = "VARCHAR";
        for (const auto& col : table->columns()) {
            if (col.name == col_name) {
                col_type = col.type;
                break;
            }
        }
        
        Value value;
        if (col_type == "INT") {
            try {
                value = stoi(value_str);
            } catch (...) {
                value = 0;
            }
        } else if (col_type == "DOUBLE") {
            try {
                value = stod(value_str);
            } catch (...) {
                value = 0.0;
            }
        } else {
            if (value_str.front() == '\'' && value_str.back() == '\'') {
                value_str = value_str.substr(1, value_str.length() - 2);
            }
            value = value_str;
        }
        
        updates[col_name] = value;
    }
    
    return updates;
}

//Part III. Realization of query prehandle helper functions.
bool processCommand(MiniSQL& db, const string& input) {
    if (input.empty()) return false;
    
    string trimmed_input = trim(input);
    
    if (trimmed_input.empty() || trimmed_input.back() != ';') {
        cout << "Error Command! Command must end with a semicolon (;)" << endl;
        cout << "Example: SELECT * FROM employees;" << endl;
        return false;
    }
    
    trimmed_input.pop_back();
    trimmed_input = trim(trimmed_input);
    
    if (trimmed_input.empty()) {
        cout << "Error Command! Empty command after removing semicolon. Please type something" << endl;
        return false;
    }
    
    string upper_input = trimmed_input;
    transform(upper_input.begin(), upper_input.end(), upper_input.begin(), ::toupper);
    
    if (upper_input == "EXIT") {
        cout << "Saving all tables to CSV..." << endl;
        db.saveAllTables();
        cout << "Thank you for using MiniSQL!" << endl;
        return true;
    }
    
    if (upper_input == "HELP") {
        showHelp();
        return false;
    }
    
    if (upper_input == "SHOW TABLES") {
        handleShowTables(db);
        return false;
    }
    
    if (upper_input.find("DROP TABLE") == 0) {
        handleDropTable(db, trimmed_input);
        return false;
    }
    
    if (upper_input.find("CREATE TABLE") == 0) {
        try {
            handleCreateTable(db, trimmed_input);
        } catch (const exception& e) {
            cout << "Error creating table: " << e.what() << endl;
        }
        return false;
    }
    
    if (upper_input.find("INSERT INTO") == 0) {
        try {
            handleInsert(db, trimmed_input);
        } catch (const exception& e) {
            cout << "Insert error: " << e.what() << endl;
        }
        return false;
    }
    
    if (upper_input.find("SELECT") == 0) {
        try {
            bool has_save_as = false;
            string save_table_name = "";
            
            string select_upper = trimmed_input;
            transform(select_upper.begin(), select_upper.end(), select_upper.begin(), ::toupper);
            
            size_t save_as_pos = select_upper.find("SAVE AS");
            if (save_as_pos != string::npos) {
                has_save_as = true;
                save_table_name = trim(trimmed_input.substr(save_as_pos + 7));
                trimmed_input = trim(trimmed_input.substr(0, save_as_pos));
            }
            
            size_t join_pos = select_upper.find("JOIN");
            if (join_pos != string::npos) {
                handleJoinSelect(db, trimmed_input, has_save_as, save_table_name);
            } else {
                handleSimpleSelect(db, trimmed_input);
            }
        } catch (const exception& e) {
            cout << "Query error: " << e.what() << endl;
        }
        return false;
    }
    if (upper_input.find("DELETE FROM") == 0) {
        try {
            handleDelete(db, trimmed_input);
        } catch (const exception& e) {
            cout << "DELETE error: " << e.what() << endl;
        }
        return false;
    }

    if (upper_input.find("UPDATE") == 0) {
        try {
            handleUpdate(db, trimmed_input);
        } catch (const exception& e) {
            cout << "UPDATE error: " << e.what() << endl;
        }
        return false;
    }
    
    cout << "Unknown command. Type HELP for available commands" << endl;
    return false;
}

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
        cout << "Error Command! No valid column definitions found" << endl;
        return;
    }
    
    db.createTable(table_name, columns, table_name + ".csv");
    
    cout << "Table created successfully. Columns: ";
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
        cout << "Error Command! INSERT INTO <table_name> VALUES (...)" << endl;
        return;
    }
    
    string table_name = trim(input.substr(11, values_pos - 11));
    if (table_name.empty()) {
        cout << "Error Cammand! Table name cannot be empty" << endl;
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
        cout << "Error Command! SELECT columns FROM <table_name>." << endl;
        return;
    }
    
    string select_part = trim(input.substr(0, from_pos));
    
    string upper_select = select_part;
    transform(upper_select.begin(), upper_select.end(), upper_select.begin(), ::toupper);
    if (upper_select.find("SELECT ") == 0) {
        select_part = select_part.substr(7); 
        select_part = trim(select_part);
    }
    
    vector<string> columns;
    string columns_str = trim(select_part);
    
    if (columns_str == "*") {
        columns.push_back("*");
    } else {
        vector<string> column_tokens = split(columns_str, ',');
        for (auto& col : column_tokens) {
            col = trim(col);
            if (!col.empty()) {
                columns.push_back(col);
            }
        }
    }
    
    if (columns.empty()) {
        cout << "Error Command! No columns specified." << endl;
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
        cout << "Error Command! Table name cannot be empty." << endl;
        return;
    }
    
    try {
        vector<Row> results = db.select(table_name, columns, {}, where_clause);
        
        auto table = db.getTable(table_name);
        if (table) {
            vector<Column> selected_columns;
            for (const auto& col_name : columns) {
                if (col_name == "*") {
                    displayResults(results, table->columns());
                    return;
                }
                for (const auto& col : table->columns()) {
                    if (col.name == col_name) {
                        selected_columns.push_back(col);
                        break;
                    }
                }
            }
            displayResults(results, selected_columns);
        } else {
            displayResults(results, {});
        }
    } catch (const exception& e) {
        cout << "Query error: " << e.what() << endl;
    }
}

void handleJoinSelect(MiniSQL& db, const string& input, bool has_save_as, const string& save_table_name) {
    // ** regular expression 
    regex pattern(R"(SELECT\s+(.*?)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(.*?)(?:\s+WHERE\s+(.*))?$)", regex::icase);
    smatch matches;
    
    if (regex_search(input, matches, pattern)) {
        string select_part = matches[1].str(); 
        string table1 = matches[2].str();
        string table2 = matches[3].str();
        string join_condition_str = matches[4].str();
        string where_str = (matches.size() > 5) ? matches[5].str() : "";
        
        // Parse SELECT columns
        vector<string> columns;
        string columns_str = trim(select_part);
        if (columns_str == "*") {
            columns.push_back("*");
        } else {
            vector<string> column_tokens = split(columns_str, ',');
            for (auto& col : column_tokens) {
                col = trim(col);
                if (!col.empty()) {
                    columns.push_back(col);
                }
            }
        }
        
        if (columns.empty()) {
            cout << "Error: No columns specified in SELECT" << endl;
            return;
        }
        
        // Parse JOIN conditions
        JoinCondition join_condition = parseJoinCondition(join_condition_str);
        if (join_condition.left_table.empty() || join_condition.right_table.empty()) {
            cout << "Error: Invalid JOIN condition format" << endl;
            return;
        }
        
        // Parse WHERE clause conditions
        shared_ptr<LogicExpression> where_clause = nullptr;
        if (!where_str.empty()) {
            auto left_table = db.getTable(table1);
            auto right_table = db.getTable(table2);
            where_clause = parseJoinWhereClause(where_str, left_table, right_table);
        }
        
        if (has_save_as) {
            bool success = db.saveJoinAsTable(save_table_name, table1, table2, join_condition, where_clause);
            if (success) {
                cout << "JOIN results saved as table: '" << save_table_name << "'" << endl;
            }
        } else {
            vector<Row> results = db.join(table1, table2, columns, JoinType::INNER_JOIN, join_condition, where_clause);
            
            vector<Column> display_columns;
            
            if (columns.size() == 1 && columns[0] == "*") {

                auto left_table_ptr = db.getTable(table1);
                auto right_table_ptr = db.getTable(table2);
                
                if (left_table_ptr) {
                    for (const auto& col : left_table_ptr->columns()) {
                        Column display_col = col;
                        display_col.name = table1 + "." + col.name;
                        display_columns.push_back(display_col);
                    }
                }
                if (right_table_ptr) {
                    for (const auto& col : right_table_ptr->columns()) {
                        Column display_col = col;
                        display_col.name = table2 + "." + col.name;
                        display_columns.push_back(display_col);
                    }
                }
            } else {
                for (const auto& col_name : columns) {
                    Column display_col;
                    display_col.name = col_name;
                    display_col.type = "VARCHAR";
                    display_col.varchar_length = 50;
                    display_columns.push_back(display_col);
                }
            }
            
            displayResults(results, display_columns);
        }
    } else {
        cout << "Error: Cannot parse JOIN query" << endl;
        cout << "Input: " << input << endl;
        return;
    }
}

void handleDropTable(MiniSQL& db, const string& input) {
    string table_name = trim(input.substr(10));
    if (table_name.empty()) {
        cout << "Error: Table name cannot be empty" << endl;
        return;
    }
    db.dropTable(table_name);
}

void handleShowTables(MiniSQL& db) {
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
}

void handleDelete(MiniSQL& db, const string& input) {
    string upper_input = input;
    transform(upper_input.begin(), upper_input.end(), upper_input.begin(), ::toupper);
    
    size_t from_pos = upper_input.find("FROM");
    if (from_pos == string::npos) {
        cout << "Syntax error: DELETE FROM <table_name> [WHERE condition]" << endl;
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
    
    try {
        int deleted_count = db.deleteRows(table_name, where_clause);
        if (deleted_count > 0) {
            cout << deleted_count << " row(s) deleted from table '" << table_name << "'" << endl;
        } else {
            cout << "No rows matched the DELETE condition" << endl;
        }
    } catch (const exception& e) {
        cout << "DELETE error: " << e.what() << endl;
    }
}

void handleUpdate(MiniSQL& db, const string& input) {
    string upper_input = input;
    transform(upper_input.begin(), upper_input.end(), upper_input.begin(), ::toupper);
    
    size_t set_pos = upper_input.find("SET");
    if (set_pos == string::npos) {
        cout << "Syntax error: UPDATE <table_name> SET column1=value1, ... [WHERE condition]" << endl;
        return;
    }
    
    size_t where_pos = upper_input.find("WHERE");
    
    string table_name = trim(input.substr(6, set_pos - 6));  // "UPDATE " 长度是7
    
    string set_clause;
    string where_str;
    
    if (where_pos != string::npos) {
        set_clause = trim(input.substr(set_pos + 3, where_pos - set_pos - 3));
        where_str = trim(input.substr(where_pos + 5));
    } else {
        set_clause = trim(input.substr(set_pos + 3));
    }
    
    if (table_name.empty()) {
        cout << "Error: Table name cannot be empty" << endl;
        return;
    }
    
    if (set_clause.empty()) {
        cout << "Error: SET clause cannot be empty" << endl;
        return;
    }
    
    auto table = db.getTable(table_name);
    if (!table) {
        cout << "Error: Table '" << table_name << "' does not exist" << endl;
        return;
    }
    
    unordered_map<string, Value> updates = parseUpdateSet(set_clause, table);
    
    if (updates.empty()) {
        cout << "Error: No valid update assignments found" << endl;
        return;
    }
    
    shared_ptr<LogicExpression> where_clause = nullptr;
    if (!where_str.empty()) {
        where_clause = parseWhereClause(where_str, table);
    }
    
    try {
        int updated_count = db.updateRows(table_name, updates, where_clause);
        if (updated_count > 0) {
            cout << updated_count << " row(s) updated in table '" << table_name << "'" << endl;
        } else {
            cout << "No rows matched the UPDATE condition" << endl;
        }
    } catch (const exception& e) {
        cout << "UPDATE error: " << e.what() << endl;
    }
}

// Part III.Realization of interface helper functions.
void displayResults(const vector<Row>& results, const vector<Column>& columns) {
    if (results.empty()) {
        cout << "No eligible records found!" << endl;
        return;
    }
    
    cout << "\nQuery results (" << results.size() << " records):" << endl;
    
    for (size_t i = 0; i < columns.size(); ++i) {
        cout << columns[i].name;
        if (columns[i].type == "VARCHAR" && columns[i].varchar_length > 0) {
            cout << "(" << columns[i].varchar_length << ")";
        }
        if (i < columns.size() - 1) cout << "\t";
    }
    cout << endl;
    
    int line_length = 0;
    for (const auto& col : columns) {
        line_length += col.name.length() + 4;
    }
    cout << string(line_length, '-') << endl;
    
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

void showHelp() {
    cout << "\nAvailable commands:" << endl;
    cout << "  CREATE TABLE <table_name> (<column_definitions>)" << endl;
    cout << "    Example: CREATE TABLE employees (id INT, name VARCHAR(50), age INT)" << endl;
    cout << endl;
    cout << "  INSERT INTO <table_name> VALUES (...)" << endl;
    cout << "    Example: INSERT INTO employees VALUES (1, 'Alice', 28)" << endl;
    cout << endl;
    cout << "  SELECT <columns> FROM <table_name> [WHERE condition]" << endl;
    cout << "    Example: SELECT * FROM employees" << endl;
    cout << "    Example: SELECT name, age FROM employees" << endl;
    cout << "    Example: SELECT name, age FROM employees WHERE age > 25" << endl;
    cout << endl;
    cout << "  UPDATE <table_name> SET column=value, ... [WHERE condition]" << endl;
    cout << "    Example: UPDATE employees SET age = 30 WHERE id = 1" << endl;
    cout << "    Example: UPDATE employees SET salary = salary * 1.1 WHERE department = 'Sales'" << endl;
    cout << endl;
    cout << "  DELETE FROM <table_name> [WHERE condition]" << endl;
    cout << "    Example: DELETE FROM employees WHERE id = 1" << endl;
    cout << "    Example: DELETE FROM employees WHERE age > 65" << endl;
    cout << endl;
    cout << "  SELECT <columns> FROM <table1> JOIN <table2> ON <condition> [WHERE condition]" << endl;
    cout << "    Example: SELECT * FROM employees JOIN departments ON employees.department_id = departments.dept_id" << endl;
    cout << "    Example: SELECT employees.name, departments.dept_name FROM employees JOIN departments ON employees.department_id = departments.dept_id" << endl;
    cout << endl;
    cout << "  DROP TABLE <table_name> - Delete a table" << endl;
    cout << "  SHOW TABLES - List all tables" << endl;
    cout << "  EXIT - Exit the program" << endl;
    cout << "  HELP - Show this help message" << endl;
}