#include "../include/minisql.h"
#include <fstream>      
#include <sstream>     
#include <algorithm>   
#include <cctype>      
#include <regex>       
#include <filesystem>  
#include <unordered_map> 
#include <iomanip>

namespace fs = std::filesystem;
string trim(const string& str);
size_t findOuterOperator(const string& expr, const string& op);

// Realization of functions defined in minisql.h
// Part I.Realization of Row class in minisql.h
Value Row::getValue(const string& column_name, const vector<string>& column_names) const {
    for (size_t i = 0; i < column_names.size(); ++i) {
        if (column_names[i] == column_name) {
            return values_[i];
        }
    }
    throw runtime_error("Column not found: " + column_name);
}

// Part II.Realization of Table class in minisql.h
Table::Table(string name, vector<Column> columns, string csv_file)
    : name_(move(name)), columns_(move(columns)), csv_file_(move(csv_file)) {
    if (!csv_file_.empty() && filesystem::exists(csv_file_)) {
        loadFromCSV();
    }
}

bool Table::loadFromCSV() {
    ifstream file(csv_file_);
    if (!file.is_open()) {
        cerr << "Fail to open: " << csv_file_ << endl;
        return false;
    }
    
    rows_.clear();
    string line;
    
    if (!getline(file, line)) {
        return true;
    }
    
    while (getline(file, line)) {
        vector<Value> row_values;
        stringstream ss(line);
        string cell;
        size_t col_idx = 0;
        
        while (getline(ss, cell, ',')) {
            if (col_idx >= columns_.size()) break;
            
            const auto& col = columns_[col_idx];
            if (col.type == "INT") {
                try {
                    row_values.push_back(stoi(cell));
                } catch (...) {
                    row_values.push_back(0);
                }
            } else if (col.type == "DOUBLE") {
                try {
                    row_values.push_back(stod(cell));
                } catch (...) {
                    row_values.push_back(0.0);
                }
            } else {
                row_values.push_back(cell);
            }
            col_idx++;
        }
        
        if (row_values.size() == columns_.size()) {
            rows_.emplace_back(move(row_values));
        }
    }
    
    file.close();
    return true;
}

bool Table::saveToCSV() {
    ofstream file(csv_file_);
    if (!file.is_open()) {
        cerr << "Fail to open: "<< csv_file_ << endl;
        return false;
    }
    
    for (size_t i = 0; i < columns_.size(); ++i) {
        file << columns_[i].name;
        if (i < columns_.size() - 1) file << ",";
    }
    file << "\n";
    
    for (const auto& row : rows_) {
        for (size_t i = 0; i < row.size(); ++i) {
            std::visit([&file](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, double>) {
                    file << std::fixed << std::setprecision(10) << arg;
                } else {
                    file << arg;
                }
            }, row[i]);
            
            if (i < row.size() - 1) file << ",";
        }
        file << "\n";
    }
    
    file.close();
    return true;
}

void Table::insertRow(const Row& row) {
    if (row.size() == columns_.size()) {
        rows_.push_back(row);
        saveToCSV();
    }
}

int Table::getColumnIndex(const string& column_name) const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].name == column_name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

vector<Row> Table::selectRows(const vector<string>& columns, const vector<string>& column_aliases, const shared_ptr<LogicExpression>& where_clause) const {
    
    vector<Row> filtered_rows = where_clause ? filterRows(where_clause) : rows_;
    // '*' means that select all colmuns
    if (columns.size() == 1 && columns[0] == "*") {
        return filtered_rows;
    }
    
    vector<Row> result;
    vector<int> column_indices;
    
    for (const auto& col_name : columns) {
        int idx = getColumnIndex(col_name);
        if (idx == -1) {
            throw runtime_error("Column not found: " + col_name);
        }
        column_indices.push_back(idx);
    }
    
    for (const auto& row : filtered_rows) {
        vector<Value> selected_values;
        for (int idx : column_indices) {
            selected_values.push_back(row[idx]);
        }
        result.emplace_back(move(selected_values));
    }
    
    return result;
}

vector<Row> Table::filterRows(const shared_ptr<LogicExpression>& where_clause) const {
    vector<Row> result;
    vector<string> column_names;
    
    for (const auto& col : columns_) {
        column_names.push_back(col.name);
    }
    
    for (const auto& row : rows_) {
        if (ConditionEvaluator::evaluate(row, column_names, where_clause)) {
            result.push_back(row);
        }
    }
    
    return result;
}

vector<Row> Table::joinTables(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause) {
    
    return JoinOptimizer::optimizeJoin(left_table, right_table, columns, join_type, condition, where_clause);
}

int Table::deleteRows(const shared_ptr<LogicExpression>& where_clause) {
    if (rows_.empty()) {
        return 0;
    }
    
    vector<string> column_names;
    for (const auto& col : columns_) {
        column_names.push_back(col.name);
    }
    
    vector<Row> remaining_rows;
    int deleted_count = 0;
    
    for (const auto& row : rows_) {
        bool should_delete = false;
        
        if (where_clause) {
            should_delete = ConditionEvaluator::evaluate(row, column_names, where_clause);
        } else {
            should_delete = true;
        }
        
        if (should_delete) {
            deleted_count++;
        } else {
            remaining_rows.push_back(row);
        }
    }
    
    if (deleted_count > 0) {
        rows_ = move(remaining_rows);
        saveToCSV(); 
    }
    
    return deleted_count;
}

int Table::updateRows(const unordered_map<string, Value>& updates, const shared_ptr<LogicExpression>& where_clause) {
    if (rows_.empty() || updates.empty()) {
        return 0;
    }
    
    vector<string> column_names;
    for (const auto& col : columns_) {
        column_names.push_back(col.name);
    }
    
    for (const auto& [col_name, _] : updates) {
        if (getColumnIndex(col_name) == -1) {
            throw runtime_error("Column '" + col_name + "' not found in table");
        }
    }
    
    int updated_count = 0;
    
    for (auto& row : rows_) {
        bool should_update = false;
        
        if (where_clause) {
            should_update = ConditionEvaluator::evaluate(row, column_names, where_clause);
        } else {
            should_update = true;
        }
        
        if (should_update) {
            for (const auto& [col_name, new_value] : updates) {
                int col_idx = getColumnIndex(col_name);
                if (col_idx != -1) {
                    row[col_idx] = new_value;
                }
            }
            updated_count++;
        }
    }
    
    if (updated_count > 0) {
        saveToCSV(); 
    }
    
    return updated_count;
}

// Part III.Realization of Queryoptimizer class in minisql.h
vector<Row> JoinOptimizer::optimizeJoin(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause) {
    
    size_t left_size = left_table.rowCount();
    size_t right_size = right_table.rowCount();
    
    return (left_size < 1000 && right_size < 1000) ? nestedLoopJoin(left_table, right_table, columns, join_type, condition, where_clause) : hashJoin(left_table, right_table, columns, join_type, condition, where_clause);
}

vector<Row> JoinOptimizer::nestedLoopJoin(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause) {
    
    vector<Row> result;
    
    vector<string> left_cols, right_cols;
    for (const auto& col : left_table.columns()) left_cols.push_back(col.name);
    for (const auto& col : right_table.columns()) right_cols.push_back(col.name);
    
    int left_idx = left_table.getColumnIndex(condition.left_column);
    int right_idx = right_table.getColumnIndex(condition.right_column);
    
    if (left_idx == -1 || right_idx == -1) {
        throw runtime_error("Join column not found");
    }
    
    if (join_type != JoinType::INNER_JOIN) {
        cout << "Warning: Only INNER JOIN is currently supported" << endl;
    }
    
    bool select_all = (columns.size() == 1 && columns[0] == "*");
    
    vector<string> all_columns_for_where = left_cols;
    all_columns_for_where.insert(all_columns_for_where.end(), right_cols.begin(), right_cols.end());
    
    for (const auto& left_row : left_table.getAllRows()) {
        for (const auto& right_row : right_table.getAllRows()) {
            bool match = false;
            
            try {
                match = ConditionEvaluator::compare(left_row[left_idx], right_row[right_idx], condition.op);
            } catch (...) {
                match = false;
            }
            
            if (match) {
                vector<Value> joined_values;
                vector<Value> all_values_for_where; 
                vector<string> where_eval_columns;
                
                if (select_all) {
                    for (const auto& val : left_row.values()) {
                        joined_values.push_back(val);
                        all_values_for_where.push_back(val);
                    }
                    for (const auto& val : right_row.values()) {
                        joined_values.push_back(val);
                        all_values_for_where.push_back(val);
                    }
                    where_eval_columns = all_columns_for_where;
                } else {
                    for (const auto& col_name : columns) {
                        size_t dot_pos = col_name.find('.');
                        if (dot_pos != string::npos) {
                            string table_name = col_name.substr(0, dot_pos);
                            string column_name = col_name.substr(dot_pos + 1);
                            if (table_name == left_table.name()) {
                                int col_idx = left_table.getColumnIndex(column_name);
                                if (col_idx != -1) {
                                    joined_values.push_back(left_row[col_idx]);
                                }
                            } else if (table_name == right_table.name()) {
                                int col_idx = right_table.getColumnIndex(column_name);
                                if (col_idx != -1) {
                                    joined_values.push_back(right_row[col_idx]);
                                }
                            }
                        } else {
                            int col_idx = left_table.getColumnIndex(col_name);
                            if (col_idx != -1) {
                                joined_values.push_back(left_row[col_idx]);
                            } else {
                                col_idx = right_table.getColumnIndex(col_name);
                                if (col_idx != -1) {
                                    joined_values.push_back(right_row[col_idx]);
                                }
                            }
                        }
                    }
                    
                    for (const auto& val : left_row.values()) {
                        all_values_for_where.push_back(val);
                    }
                    for (const auto& val : right_row.values()) {
                        all_values_for_where.push_back(val);
                    }
                    where_eval_columns = all_columns_for_where;
                }
                
                Row joined_row(joined_values);
                Row where_eval_row(all_values_for_where); 
                
                if (!where_clause || ConditionEvaluator::evaluate(where_eval_row, where_eval_columns, where_clause)) {
                    result.push_back(joined_row);
                }
            }
        }
    }
    
    return result;
}

vector<Row> JoinOptimizer::hashJoin(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause) {
    
    // Choose the smaller table as the build table.
    size_t left_size = left_table.rowCount();
    size_t right_size = right_table.rowCount();
    const Table& build_table = (left_size <= right_size) ? left_table : right_table;
    const Table& probe_table = (left_size <= right_size) ? right_table : left_table;
    
    int build_idx, probe_idx;
    if (&build_table == &left_table) {
        build_idx = build_table.getColumnIndex(condition.left_column);
        probe_idx = probe_table.getColumnIndex(condition.right_column);
    } else {
        build_idx = build_table.getColumnIndex(condition.right_column);
        probe_idx = probe_table.getColumnIndex(condition.left_column);
    }
    
    if (build_idx == -1 || probe_idx == -1) {
        throw runtime_error("Join column not found");
    }

    bool select_all = (columns.size() == 1 && columns[0] == "*");
    
    vector<string> left_cols, right_cols;
    for (const auto& col : left_table.columns()) left_cols.push_back(col.name);
    for (const auto& col : right_table.columns()) right_cols.push_back(col.name);
    
    vector<string> all_columns_for_where = left_cols;
    all_columns_for_where.insert(all_columns_for_where.end(), right_cols.begin(), right_cols.end());
    
    // construct hash table
    unordered_multimap<Value, const Row*> hash_table;
    for (const auto& row : build_table.getAllRows()) {
        hash_table.insert({row[build_idx], &row});
    }
    
    vector<Row> result;
    
    // scan probe table(the larger one)
    for (const auto& probe_row : probe_table.getAllRows()) {
        const Value& probe_key = probe_row[probe_idx];
        
        auto range = hash_table.equal_range(probe_key);
        for (auto it = range.first; it != range.second; ++it) {
            const Row* build_row = it->second;
            
            vector<Value> joined_values;
            vector<Value> all_values_for_where; 
            vector<string> where_eval_columns;
            
            if (select_all) {
                if (&build_table == &left_table) {
                    for (const auto& val : build_row->values()) {
                        joined_values.push_back(val);
                        all_values_for_where.push_back(val);
                    }
                    for (const auto& val : probe_row.values()) {
                        joined_values.push_back(val);
                        all_values_for_where.push_back(val);
                    }
                    where_eval_columns = all_columns_for_where;
                } else {
                    for (const auto& val : probe_row.values()) {
                        joined_values.push_back(val);
                        all_values_for_where.push_back(val);
                    }
                    for (const auto& val : build_row->values()) {
                        joined_values.push_back(val);
                        all_values_for_where.push_back(val);
                    }
                    where_eval_columns = all_columns_for_where;
                }
            } else {
                for (const auto& col_name : columns) {
                    bool found = false;
                    size_t dot_pos = col_name.find('.');
                    if (dot_pos != string::npos) {
                        string table_name = col_name.substr(0, dot_pos);
                        string column_name = col_name.substr(dot_pos + 1);
                        if (table_name == left_table.name()) {
                            int col_idx = left_table.getColumnIndex(column_name);
                            if (col_idx != -1) {
                                joined_values.push_back((&build_table == &left_table) ? (*build_row)[col_idx] : probe_row[col_idx]);
                                found = true;
                            }
                        } else if (table_name == right_table.name()) {
                            int col_idx = right_table.getColumnIndex(column_name);
                            if (col_idx != -1) {
                                joined_values.push_back((&build_table == &right_table) ? (*build_row)[col_idx] : probe_row[col_idx]);
                                found = true;
                            }
                        }
                    } else {
                        int col_idx = left_table.getColumnIndex(col_name);
                        if (col_idx != -1) {
                            joined_values.push_back((&build_table == &left_table) ? (*build_row)[col_idx] : probe_row[col_idx]);
                            found = true;
                        } else {
                            col_idx = right_table.getColumnIndex(col_name);
                            if (col_idx != -1) {
                                joined_values.push_back((&build_table == &right_table) ? (*build_row)[col_idx] : probe_row[col_idx]);
                                found = true;
                            }
                        }
                    }
    
                    if (!found) {
                        cout << "Warning: Column '" << col_name << "' not found in join tables" << endl;
                    }
                }

                if (&build_table == &left_table) {
                    for (const auto& val : build_row->values()) {
                        all_values_for_where.push_back(val);
                    }
                    for (const auto& val : probe_row.values()) {
                        all_values_for_where.push_back(val);
                    }
                } else {
                    for (const auto& val : probe_row.values()) {
                        all_values_for_where.push_back(val);
                    }
                    for (const auto& val : build_row->values()) {
                        all_values_for_where.push_back(val);
                    }
                }
                where_eval_columns = all_columns_for_where;
            }

            Row joined_row(joined_values);
            Row where_eval_row(all_values_for_where);
            
            if (!where_clause || ConditionEvaluator::evaluate(where_eval_row, where_eval_columns, where_clause)) {
                result.push_back(joined_row);
            }
        }
    }
    
    return result;
}

// Part IV.Realization of ConditionEvaluator class in minisql.h
template<typename T>
bool ConditionEvaluator::compareValues(const T& left, const T& right, CompareOp op) {
    switch (op) {
        case CompareOp::EQUAL: return left == right;
        case CompareOp::NOT_EQUAL: return left != right;
        case CompareOp::GREATER: return left > right;
        case CompareOp::LESS: return left < right;
        case CompareOp::GREATER_EQUAL: return left >= right;
        case CompareOp::LESS_EQUAL: return left <= right;
        default: return false;
    }
}

bool ConditionEvaluator::compare(const Value& left, const Value& right, CompareOp op) {
    return visit([&right, op](auto&& left_val) {
        return visit([&left_val, op](auto&& right_val) {
            using LeftType = decay_t<decltype(left_val)>;
            using RightType = decay_t<decltype(right_val)>;
            
            if constexpr (is_same_v<LeftType, RightType>) {
                return compareValues(left_val, right_val, op);
            } else if constexpr (is_arithmetic_v<LeftType> && is_arithmetic_v<RightType>) {
                double left_double = static_cast<double>(left_val);
                double right_double = static_cast<double>(right_val);
                return compareValues(left_double, right_double, op);
            } else {
                return false;
            }
        }, right);
    }, left);
}

bool ConditionEvaluator::evaluate(const Row& row, const vector<string>& column_names, const Condition& condition) {
    try {
        Value left_value = row.getValue(condition.left_column, column_names);
        
        if (condition.is_column_comparison) {
            Value right_value = row.getValue(condition.right_column, column_names);
            return compare(left_value, right_value, condition.op);
        } else {
            return compare(left_value, condition.constant_value, condition.op);
        }
    } catch (...) {
        return false;
    }
}

bool ConditionEvaluator::evaluate(const Row& row, const vector<string>& column_names, const shared_ptr<LogicExpression>& expression) {
    if (!expression) return false;
    
    if (expression->isSingleCondition) {
        if (holds_alternative<Condition>(expression->left)) {
            return evaluate(row, column_names, get<Condition>(expression->left));
        }
        return false;
    }
    
    bool left_result = false;
    if (holds_alternative<Condition>(expression->left)) {
        left_result = evaluate(row, column_names, get<Condition>(expression->left));
    } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->left)) {
        left_result = evaluate(row, column_names, get<shared_ptr<LogicExpression>>(expression->left));
    }
    
    if (expression->op == LogicOp::NOT) {
        return !left_result;
    }
    
    bool right_result = false;
    if (holds_alternative<Condition>(expression->right)) {
        right_result = evaluate(row, column_names, get<Condition>(expression->right));
    } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->right)) {
        right_result = evaluate(row, column_names, get<shared_ptr<LogicExpression>>(expression->right));
    }
    
    switch (expression->op) {
        case LogicOp::AND: return left_result && right_result;
        case LogicOp::OR: return left_result || right_result;
        default: return false;
    }
}

// Part V. Realization of WhereParser class in minisql.h
shared_ptr<LogicExpression> WhereParser::parse(const string& where_str, const vector<Column>& columns) {
    string str = trim(where_str);
    
    if (!validateExpression(str)) {
        cerr << "Error: Invalid WHERE expression syntax: " << where_str << endl;
        return nullptr;
    }
    
    return parseExpression(str, columns);
}

Value WhereParser::parseValue(const string& str, const string& type) {
    if (type == "INT") {
        try {
            return stoi(str);
        } catch (...) {
            return 0;
        }
    } else if (type == "DOUBLE") {
        try {
            return stod(str);
        } catch (...) {
            return 0.0;
        }
    } else {
        if (str.front() == '\'' && str.back() == '\'') {
            return str.substr(1, str.length() - 2);
        }
        return str;
    }
}

CompareOp WhereParser::parseCompareOp(const string& op_str) {
    if (op_str == "=") return CompareOp::EQUAL;
    if (op_str == "<>" || op_str == "!=") return CompareOp::NOT_EQUAL;
    if (op_str == ">") return CompareOp::GREATER;
    if (op_str == "<") return CompareOp::LESS;
    if (op_str == ">=") return CompareOp::GREATER_EQUAL;
    if (op_str == "<=") return CompareOp::LESS_EQUAL;
    return CompareOp::EQUAL;
}

LogicOp WhereParser::parseLogicOp(const string& op_str) {
    string upper_op = op_str;
    transform(upper_op.begin(), upper_op.end(), upper_op.begin(), ::toupper);
    
    if (upper_op == "AND") return LogicOp::AND;
    if (upper_op == "OR") return LogicOp::OR;
    if (upper_op == "NOT") return LogicOp::NOT;
    return LogicOp::AND;
}

string WhereParser::parseColumnName(const string& column_ref, const vector<Column>& columns) {
    string column_name_only = column_ref;
    
    size_t dot_pos = column_ref.find('.');
    if (dot_pos != string::npos) {
        column_name_only = column_ref.substr(dot_pos + 1);
    }
    
    for (const auto& col : columns) {
        if (col.name == column_name_only) {
            return column_name_only;
        }
    }
    
    return "";
}

shared_ptr<LogicExpression> WhereParser::parseSingleCondition(const string& condition_str, const vector<Column>& columns) {
    string str = trim(condition_str);
    
    if (str.empty()) {
        return nullptr;
    }
    
    regex pattern(R"(([\w\.]+)\s*([=<>!]+)\s*('?[^']*'?|\d+\.?\d*|[\w\.]+))");
    smatch matches;
    
    if (!regex_search(str, matches, pattern) || matches.size() < 4) {
        cerr << "Error: Invalid condition format: " << str << endl;
        return nullptr;
    }
    
    string left_column_full = matches[1].str();
    string op = matches[2].str();
    string right_part = matches[3].str();
    
    string left_column_name = parseColumnName(left_column_full, columns);
    if (left_column_name.empty()) {
        cerr << "Error: Left column '" << left_column_full << "' not found in tables" << endl;
        return nullptr;
    }
    
    string col_type = "VARCHAR";
    for (const auto& col : columns) {
        if (col.name == left_column_name) {
            col_type = col.type;
            break;
        }
    }
    
    bool is_column_comparison = false;
    string right_column_name = "";
    
    bool is_quoted_string = (right_part.front() == '\'' && right_part.back() == '\'');
    bool is_number = !right_part.empty() && all_of(right_part.begin(), right_part.end(), [](char c) {
        return isdigit(c) || c == '.' || c == '-' || c == '+';
    });
    
    if (!is_quoted_string && !is_number) {
        right_column_name = parseColumnName(right_part, columns);
        if (!right_column_name.empty()) {
            is_column_comparison = true;
        }
    }
    
    auto expression = make_shared<LogicExpression>();
    Condition condition;
    condition.left_column = left_column_name;
    condition.op = parseCompareOp(op);
    condition.is_column_comparison = is_column_comparison;
    
    if (is_column_comparison) {
        condition.right_column = right_column_name;
    } else {
        condition.constant_value = parseValue(right_part, col_type);
    }
    
    expression->isSingleCondition = true;
    expression->left = condition;
    expression->op = LogicOp::AND;
    
    return expression;
}

//** Difficult part: parse condition expression, including finding syntax error.
shared_ptr<LogicExpression> WhereParser::parseExpression(const string& expr_str, const vector<Column>& columns) {
    string str = trim(expr_str);
    
    if (str.empty()) {
        cerr << "Error: Empty expression" << endl;
        return nullptr;
    }
    
    //Parsing the OR operator
    size_t or_pos = findOuterOperator(str, "OR");
    if (or_pos != string::npos) {
        string left_str = trim(str.substr(0, or_pos));
        string right_str = trim(str.substr(or_pos + 2)); 
        
        if (left_str.empty() || right_str.empty()) {
            cerr << "Error: Missing operand for OR operator" << endl;
            return nullptr;
        }
        
        auto left_expr = parseExpression(left_str, columns);
        auto right_expr = parseExpression(right_str, columns);
        
        if (!left_expr || !right_expr) {
            return nullptr;
        }
        
        auto expr = make_shared<LogicExpression>();
        expr->op = LogicOp::OR;
        expr->isSingleCondition = false;
        expr->left = left_expr;
        expr->right = right_expr;
        
        return expr;
    }
    
    // Parsing the AND operator (considering parentheses using findOuterOperator)
    size_t and_pos = findOuterOperator(str, "AND");
    if (and_pos != string::npos) {
        string left_str = trim(str.substr(0, and_pos));
        string right_str = trim(str.substr(and_pos + 3));
        
        if (left_str.empty() || right_str.empty()) {
            cerr << "Error: Missing operand for AND operator" << endl;
            return nullptr;
        }
        
        auto left_expr = parseExpression(left_str, columns);
        auto right_expr = parseExpression(right_str, columns);
        
        if (!left_expr || !right_expr) {
            return nullptr;
        }
        
        auto expr = make_shared<LogicExpression>();
        expr->op = LogicOp::AND;
        expr->isSingleCondition = false;
        expr->left = left_expr;
        expr->right = right_expr;
        
        return expr;
    }
    
    // Parsing the NOT operator
    string upper_str = str;
    transform(upper_str.begin(), upper_str.end(), upper_str.begin(), ::toupper);
    
    if (upper_str.size() >= 3 && upper_str.substr(0, 3) == "NOT") {
        size_t not_end = 3;
        while (not_end < str.size() && isspace(str[not_end])) {
            not_end++;
        }
        
        if (not_end >= str.size()) {
            cerr << "Command Error! Missing operand for NOT operator" << endl;
            return nullptr;
        }
        
        string inner_str = trim(str.substr(not_end));
        if (inner_str.empty()) {
            cerr << "Command Error! Empty expression after NOT" << endl;
            return nullptr;
        }
        
        if (inner_str.front() == '(' && inner_str.back() == ')') {
            inner_str = inner_str.substr(1, inner_str.length() - 2);
            inner_str = trim(inner_str);
        }
        
        auto inner_expr = parseExpression(inner_str, columns);
        if (!inner_expr) {
            return nullptr;
        }
        
        auto expr = make_shared<LogicExpression>();
        expr->op = LogicOp::NOT;
        expr->isSingleCondition = false;
        expr->left = inner_expr;
        
        return expr;
    }
    
    // Parsing parentheses expressions
    if (str.front() == '(' && str.back() == ')') {
        string inner_str = str.substr(1, str.length() - 2);
        
        if (trim(inner_str).empty()) {
            cerr << "Error: Empty expression inside parentheses" << endl;
            return nullptr;
        }
        
        return parseExpression(inner_str, columns);
    }
    
    // Parsing NOT in parentheses, such as (NOT age > 25)
    if (str.size() > 4 && str.front() == '(' && 
        toupper(str[1]) == 'N' && toupper(str[2]) == 'O' && toupper(str[3]) == 'T') {
        
        size_t not_start = 4;
        while (not_start < str.size() && isspace(str[not_start])) {
            not_start++;
        }
        
        if (not_start < str.size() && str.back() == ')') {
            string inner_without_not = str.substr(not_start, str.length() - not_start - 1);
            string inner_str = trim(inner_without_not);
            
            if (!inner_str.empty()) {
                auto inner_expr = parseExpression(inner_str, columns);
                if (!inner_expr) {
                    return nullptr;
                }
                
                auto expr = make_shared<LogicExpression>();
                expr->op = LogicOp::NOT;
                expr->isSingleCondition = false;
                expr->left = inner_expr;
                
                return expr;
            }
        }
    }
    
    // SingleCondition
    return parseSingleCondition(str, columns);
}

bool WhereParser::validateExpression(const string& expr_str) {
    string str = trim(expr_str);
    if (str.empty()) {
        cerr << "Error: Empty WHERE expression" << endl;
        return false;
    }
    
    int paren_count = 0;
    for (char c : str) {
        if (c == '(') paren_count++;
        else if (c == ')') paren_count--;
        if (paren_count < 0) {
            cerr << "Error: Unmatched ')'" << endl;
            return false;
        }
    }
    if (paren_count != 0) {
        cerr << "Error: Unmatched parentheses" << endl;
        return false;
    }
    
    return true;
}

bool WhereParser::validateCondition(const string& cond_str) {
    regex pattern(R"(\s*([\w\.]+)\s*([=<>!]+)\s*('?[^']*'?|\d+\.?\d*|[\w\.]+)\s*)");
    return regex_match(cond_str, pattern);
}

// Part VI.Realization of BufferPool class in minisql.h
shared_ptr<Table> BufferPool::getTable(const string& table_name) {
    auto it = cache_.find(table_name);
    if (it != cache_.end()) {
        auto order_it = find(access_order_.begin(), access_order_.end(), table_name);
        if (order_it != access_order_.end()) {
            access_order_.erase(order_it);
            access_order_.push_back(table_name);
        }
        return it->second;
    }
    return nullptr;
}

void BufferPool::putTable(const string& table_name, shared_ptr<Table> table) {
    if (cache_.size() >= capacity_) {
        evictLRU();
    }
    
    cache_[table_name] = table;
    access_order_.push_back(table_name);
}

bool BufferPool::removeTable(const string& table_name) {
    auto it = cache_.find(table_name);
    if (it != cache_.end()) {
        it->second->saveToCSV();
        
        auto order_it = find(access_order_.begin(), access_order_.end(), table_name);
        if (order_it != access_order_.end()) {
            access_order_.erase(order_it);
        }
        cache_.erase(it);
        return true;
    }
    return false;
}

void BufferPool::saveAllTables() {
    for (auto& [name, table] : cache_) {
        table->saveToCSV();
    }
}

void BufferPool::evictLRU() {
    if (!access_order_.empty()) {
        string lru_table = access_order_.front();
        access_order_.erase(access_order_.begin());
        
        auto it = cache_.find(lru_table);
        if (it != cache_.end()) {
            it->second->saveToCSV();
            cache_.erase(it);
        }
    }
}

// Part VII. Realization of MiniSQL class in minisql.h
MiniSQL::MiniSQL() {
    buffer_pool_ = make_unique<BufferPool>(100);
    loadAllTablesFromDisk();
}

void MiniSQL::createTable(const string& name, const vector<Column>& columns, const string& csv_file) {
    
    if (buffer_pool_->hasTable(name)) {
        cout << "Error: Table '" << name << "' already exists in memory." << endl;
        return;
    }
    
    string csv_path = "../../data/" + (csv_file.empty() ? name + ".csv" : csv_file);
    
    error_code ec;
    if (filesystem::exists(csv_path, ec)) {
        cout << "Warning: CSV file '" << csv_path << "' already exists." << endl;
        cout << "Loading existing data instead of creating new table..." << endl;
        
        if (loadTableFromDisk(name, csv_path)) {
            cout << "Table '" << name << "' loaded from existing CSV file." << endl;
        } else {
            cout << "Failed to load table from existing CSV." << endl;
        }
        return;
    }
    
    filesystem::create_directories("../../data", ec);
    
    ofstream file(csv_path);
    if (file.is_open()) {
        for (size_t i = 0; i < columns.size(); ++i) {
            file << columns[i].name;
            if (i < columns.size() - 1) file << ",";
        }
        file << "\n";
        file.close();
    } else {
        cerr << "Error: Cannot create CSV file: " << csv_path << endl;
        return;
    }
    
    auto table = make_shared<Table>(name, columns, csv_path);
    
    buffer_pool_->putTable(name, table);
    tables_[name] = table;
    
    cout << "Table '" << name << "' created successfully with " << columns.size() << " columns." << endl;
}

void MiniSQL::saveAllTables() {
    buffer_pool_->saveAllTables();
}

vector<string> MiniSQL::listTables() const {
    vector<string> all_tables;
    
    for (const auto& pair : tables_) {
        all_tables.push_back(pair.first);
    }
    
    auto disk_tables = getTableNamesFromDisk();
    
    for (const auto& table_name : disk_tables) {
        if (find(all_tables.begin(), all_tables.end(), table_name) == all_tables.end()) {
            all_tables.push_back(table_name);
        }
    }
    
    sort(all_tables.begin(), all_tables.end());
    
    return all_tables;
}

bool MiniSQL::dropTable(const string& table_name) {
    bool in_memory = tables_.find(table_name) != tables_.end();
    bool on_disk = false;
    
    string csv_file = "../../data/" + table_name + ".csv";
    error_code ec;
    if (filesystem::exists(csv_file, ec)) {
        on_disk = true;
    }
    
    if (!in_memory && !on_disk) {
        cerr << "Error: Table '" << table_name << "' does not exist" << endl;
        return false;
    }
    
    if (in_memory) {
        auto it = tables_.find(table_name);
        if (it != tables_.end()) {
            buffer_pool_->removeTable(table_name);
            tables_.erase(it);
        }
    }
    
    if (on_disk) {
        if (!filesystem::remove(csv_file, ec)) {
            cerr << "Fail to delete CSV file: " << csv_file << endl;
            
            if (ec.value() == EACCES || ec.value() == EPERM) {
                cerr << "File is open now. Please close the file and try again." << endl;
            }
            
            if (in_memory) {
                cout << "Table has been removed from memory." << endl;
            }
            
            return false;
        }
    }
    
    cout << "Table '" << table_name << "' dropped successfully!" << endl;
    return true;
}

bool MiniSQL::insert(const string& table_name, const Row& row) {
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        return false;
    }
    
    table->insertRow(row);
    return true;
}

vector<Row> MiniSQL::select(const string& table_name, const vector<string>& columns, const vector<string>& column_aliases, const shared_ptr<LogicExpression>& where_clause) {
    
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        return {};
    }
    
    vector<string> aliases = column_aliases.empty() ? columns : column_aliases;
    return table->selectRows(columns, aliases, where_clause);
}

vector<Row> MiniSQL::join(const string& left_table, const string& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause) {
    
    auto left_table_ptr = buffer_pool_->getTable(left_table);
    auto right_table_ptr = buffer_pool_->getTable(right_table);
    
    if (!left_table_ptr || !right_table_ptr) {
        return {};
    }
    
    return Table::joinTables(*left_table_ptr, *right_table_ptr, columns, join_type, condition, where_clause);
}

bool MiniSQL::saveJoinAsTable(const string& new_table_name, const string& left_table_name, const string& right_table_name, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause) {
    
    return createTableFromJoin(new_table_name, left_table_name, right_table_name, JoinType::INNER_JOIN, condition, where_clause);
}

shared_ptr<Table> MiniSQL::getTable(const string& table_name) {
    return buffer_pool_->getTable(table_name);
}

int MiniSQL::deleteRows(const string& table_name, const shared_ptr<LogicExpression>& where_clause) {
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        cerr << "Error: Table '" << table_name << "' does not exist" << endl;
        return 0;
    }
    
    int deleted_count = table->deleteRows(where_clause);
    return deleted_count;
}

int MiniSQL::updateRows(const string& table_name, const unordered_map<string, Value>& updates, const shared_ptr<LogicExpression>& where_clause) {
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        cerr << "Error: Table '" << table_name << "' does not exist" << endl;
        return 0;
    }
    
    if (updates.empty()) {
        cout << "Warning: No columns to update" << endl;
        return 0;
    }
    
    try {
        int updated_count = table->updateRows(updates, where_clause);
        return updated_count;
    } catch (const exception& e) {
        cerr << "Update error: " << e.what() << endl;
        return 0;
    }
}

bool MiniSQL::tableExists(const string& table_name) const {
    return tables_.find(table_name) != tables_.end();
}

bool MiniSQL::createTableFromJoin(const string& new_table_name, const string& left_table_name, const string& right_table_name, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause) {
    
    if (tableExists(new_table_name)) {
        cerr << "Error: Table '" << new_table_name << "' already exists" << endl;
        return false;
    }
    
    auto left_table = getTable(left_table_name);
    auto right_table = getTable(right_table_name);
    
    if (!left_table || !right_table) {
        cerr << "Error: One or both tables do not exist" << endl;
        return false;
    }
    
    auto results = join(left_table_name, right_table_name, {"*"}, join_type, condition, where_clause);
    
    vector<Column> merged_columns;
    
    for (const auto& col : left_table->columns()) {
        Column new_col = col;
        new_col.name = left_table->name() + "_" + col.name;
        merged_columns.push_back(new_col);
    }
    
    for (const auto& col : right_table->columns()) {
        Column new_col = col;
        new_col.name = right_table->name() + "_" + col.name;
        merged_columns.push_back(new_col);
    }
    
    createTable(new_table_name, merged_columns, new_table_name + ".csv");
    
    auto new_table = getTable(new_table_name);
    if (new_table) {
        new_table->clearRows();
        for (const auto& row : results) {
            new_table->insertRow(row);
        }
        cout << "Created table '" << new_table_name << "' with " 
              << results.size() << " rows from JOIN" << endl;
        return true;
    }
    
    return false;
}

vector<string> MiniSQL::getCSVFilesInDataDir() const {
    vector<string> csv_files;
    string data_dir = "../../data/";
    error_code ec;
    
    if (!filesystem::exists(data_dir, ec)) {
        return csv_files;
    }
    
    for (const auto& entry : filesystem::directory_iterator(data_dir)) {
        if (entry.is_regular_file()) {
            string filename = entry.path().filename().string();
            if (filename.size() >= 4 && filename.substr(filename.size() - 4) == ".csv") {
                csv_files.push_back(filename);
            }
        }
    }
    
    return csv_files;
}

vector<string> MiniSQL::getTableNamesFromDisk() const {
    vector<string> table_names;
    auto csv_files = getCSVFilesInDataDir();
    
    for (const auto& csv_file : csv_files) {
        table_names.push_back(csv_file.substr(0, csv_file.size() - 4));
    }
    
    return table_names;
}

void MiniSQL::loadAllTablesFromDisk() {
    string data_dir = "../../data/";
    error_code ec;
    
    if (!filesystem::exists(data_dir, ec)) {
        return;
    }
    
    for (const auto& entry : filesystem::directory_iterator(data_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            string csv_file = entry.path().filename().string();
            string table_name = csv_file.substr(0, csv_file.size() - 4);
            
            if (!buffer_pool_->hasTable(table_name)) {
                loadTableFromDisk(table_name, entry.path().string());
            }
        }
    }
}

bool MiniSQL::loadTableFromDisk(const string& table_name, const string& csv_path) {
    try {
        ifstream file(csv_path);
        if (!file.is_open()) {
            return false;
        }
        
        string header;
        if (!getline(file, header)) {
            return false;
        }
        
        //Determine the data type by reading 5 lines and check their types.
        vector<string> sample_rows;
        string line;
        for (int i = 0; i < 5 && getline(file, line); ++i) {
            sample_rows.push_back(line);
        }
        file.close();
        
        vector<string> col_names;
        stringstream ss(header);
        string col_name;
        while (getline(ss, col_name, ',')) {
            col_names.push_back(trim(col_name));
        }
        
        auto inferColumnType = [&sample_rows](size_t col_index) -> string {
            if (sample_rows.empty()) return "VARCHAR";
            
            bool all_integers = true;
            bool all_numbers = true; 
            
            for (const auto& row : sample_rows) {
                vector<string> cells;
                stringstream ss_row(row);
                string cell;
                size_t current_idx = 0;
                
                while (getline(ss_row, cell, ',')) {
                    if (current_idx == col_index) {
                        cell = trim(cell);
                        
                        if (!cell.empty()) {
                            bool is_integer = true;
                            bool has_digits = false;
                            
                            size_t start = 0;
                            if (!cell.empty() && (cell[0] == '-' || cell[0] == '+')) {
                                start = 1;
                            }
                            
                            for (size_t i = start; i < cell.size(); ++i) {
                                if (!isdigit(static_cast<unsigned char>(cell[i]))) {
                                    is_integer = false;
                                    break;
                                }
                                has_digits = true;
                            }
                            
                            if (!is_integer || !has_digits) {
                                all_integers = false;
                            }
                            
                            bool is_number = true;
                            bool has_decimal_point = false;
                            has_digits = false;
                            start = 0;
                            
                            if (!cell.empty() && (cell[0] == '-' || cell[0] == '+')) {
                                start = 1;
                            }
                            
                            for (size_t i = start; i < cell.size(); ++i) {
                                char c = cell[i];
                                if (!isdigit(static_cast<unsigned char>(c)) && c != '.') {
                                    is_number = false;
                                    break;
                                }
                                if (c == '.') {
                                    if (has_decimal_point) {
                                        is_number = false;
                                        break;
                                    }
                                    has_decimal_point = true;
                                }
                                if (isdigit(static_cast<unsigned char>(c))) {
                                    has_digits = true;
                                }
                            }
                            
                            if (!is_number || !has_digits) {
                                all_numbers = false;
                            }
                        } else {
                            all_integers = false;
                            all_numbers = false;
                        }
                        break;
                    }
                    current_idx++;
                }
            }
            
            if (all_integers) {
                return "INT";
            } else if (all_numbers) {
                return "DOUBLE"; 
            } else {
                return "VARCHAR";
            }
        };
                
        vector<Column> columns;
        for (size_t i = 0; i < col_names.size(); ++i) {
            Column col;
            col.name = col_names[i];
            col.type = inferColumnType(i);
            
            if (col.type == "VARCHAR") {
                size_t max_length = 50;
                for (const auto& row : sample_rows) {
                    vector<string> cells;
                    stringstream ss_row(row);
                    string cell;
                    size_t current_idx = 0;
                    
                    while (getline(ss_row, cell, ',')) {
                        if (current_idx == i) {
                            max_length = max(max_length, cell.length());
                            break;
                        }
                        current_idx++;
                    }
                }
                col.varchar_length = min(max_length, static_cast<size_t>(255));
            }
            
            columns.push_back(col);
        }
        
        auto table = make_shared<Table>(table_name, columns, csv_path);
        
        buffer_pool_->putTable(table_name, table);
        tables_[table_name] = table;
        
        return true;
        
    } catch (...) {
        return false;
    }
}