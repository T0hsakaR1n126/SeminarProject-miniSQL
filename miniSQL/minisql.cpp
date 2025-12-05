#include "minisql.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <regex>
#include <filesystem>
#include <cmath>
#include <thread>
#include <cerrno>

namespace fs = std::filesystem;

// ==================== Row类的实现 ====================
Value Row::getValue(const string& column_name, const vector<string>& column_names) const {
    for (size_t i = 0; i < column_names.size(); ++i) {
        if (column_names[i] == column_name) {
            return values_[i];
        }
    }
    throw runtime_error("Column not found: " + column_name);
}

// ==================== QueryOptimizer 实现 ====================
QueryOptimizer::QueryPlan QueryOptimizer::optimizeSelect(
    const string& table_name,
    const shared_ptr<LogicExpression>& where_clause,
    size_t table_row_count) {
    
    QueryPlan plan;
    
    if (!where_clause) {
        plan.description = "Full table scan on " + table_name;
        plan.estimated_rows = table_row_count;
        plan.estimated_cost = table_row_count * 0.1;
    } else {
        double selectivity = estimateSelectivity(where_clause);
        plan.estimated_rows = static_cast<size_t>(table_row_count * selectivity);
        
        if (selectivity < 0.1) {
            plan.description = "Filtered scan on " + table_name + 
                              " (high selectivity: " + to_string(selectivity) + ")";
            plan.estimated_cost = table_row_count * 0.05 + plan.estimated_rows * 0.02;
        } else {
            plan.description = "Filtered scan on " + table_name + 
                              " (low selectivity: " + to_string(selectivity) + ")";
            plan.estimated_cost = table_row_count * 0.1 + plan.estimated_rows * 0.05;
        }
    }
    
    return plan;
}

QueryOptimizer::QueryPlan QueryOptimizer::optimizeJoin(
    const string& left_table,
    const string& right_table,
    const JoinCondition& join_condition,
    const shared_ptr<LogicExpression>& where_clause,
    size_t left_row_count,
    size_t right_row_count) {
    
    QueryPlan plan;
    
    double join_selectivity = 0.1;
    size_t join_rows = static_cast<size_t>(left_row_count * right_row_count * join_selectivity);
    
    double where_selectivity = 1.0;
    if (where_clause) {
        where_selectivity = estimateSelectivity(where_clause);
    }
    
    plan.estimated_rows = static_cast<size_t>(join_rows * where_selectivity);
    
    if (left_row_count < 1000 && right_row_count < 1000) {
        plan.description = "Nested loop join: " + left_table + " ⋈ " + right_table + 
                          " on " + join_condition.left_column + " = " + join_condition.right_column;
        plan.estimated_cost = left_row_count * right_row_count * 0.01;
    } else {
        plan.description = "Hash join (recommended): " + left_table + " ⋈ " + right_table + 
                          " on " + join_condition.left_column + " = " + join_condition.right_column;
        plan.estimated_cost = (left_row_count + right_row_count) * 0.05;
    }
    
    if (where_clause) {
        plan.description += " with WHERE filter";
        plan.estimated_cost += plan.estimated_rows * 0.02;
    }
    
    return plan;
}

double QueryOptimizer::estimateSelectivity(const Condition& condition) {
    switch (condition.op) {
        case CompareOp::EQUAL: return 0.01;
        case CompareOp::NOT_EQUAL: return 0.99;
        case CompareOp::GREATER:
        case CompareOp::LESS:
        case CompareOp::GREATER_EQUAL:
        case CompareOp::LESS_EQUAL: return 0.3;
        default: return 0.5;
    }
}

double QueryOptimizer::estimateSelectivity(const shared_ptr<LogicExpression>& expression) {
    if (!expression) return 1.0;
    
    if (expression->isSingleCondition && 
        holds_alternative<Condition>(expression->left)) {
        
        const auto& condition = get<Condition>(expression->left);
        return estimateSelectivity(condition);
    }
    
    if (expression->op == LogicOp::AND) {
        double left_sel = 0.0, right_sel = 0.0;
        
        if (holds_alternative<Condition>(expression->left)) {
            left_sel = estimateSelectivity(get<Condition>(expression->left));
        } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->left)) {
            left_sel = estimateSelectivity(get<shared_ptr<LogicExpression>>(expression->left));
        }
        
        if (holds_alternative<Condition>(expression->right)) {
            right_sel = estimateSelectivity(get<Condition>(expression->right));
        } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->right)) {
            right_sel = estimateSelectivity(get<shared_ptr<LogicExpression>>(expression->right));
        }
        
        return left_sel * right_sel;
    } else if (expression->op == LogicOp::OR) {
        double left_sel = 0.0, right_sel = 0.0;
        
        if (holds_alternative<Condition>(expression->left)) {
            left_sel = estimateSelectivity(get<Condition>(expression->left));
        } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->left)) {
            left_sel = estimateSelectivity(get<shared_ptr<LogicExpression>>(expression->left));
        }
        
        if (holds_alternative<Condition>(expression->right)) {
            right_sel = estimateSelectivity(get<Condition>(expression->right));
        } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->right)) {
            right_sel = estimateSelectivity(get<shared_ptr<LogicExpression>>(expression->right));
        }
        
        return min(1.0, left_sel + right_sel - (left_sel * right_sel));
    }
    
    return 0.5;
}

// ==================== Table 类实现 ====================
Table::Table(string name, vector<Column> columns, string csv_file)
    : name_(move(name)), columns_(move(columns)), csv_file_(move(csv_file)) {
    if (!csv_file_.empty() && filesystem::exists(csv_file_)) {
        loadFromCSV();
    }
}

bool Table::loadFromCSV() {
    ifstream file(csv_file_);
    if (!file.is_open()) {
        cerr << "Cannot open CSV file: " << csv_file_ << endl;
        return false;
    }
    
    rows_.clear();
    string line;
    
    // 跳过表头
    if (!getline(file, line)) {
        return true; // 空文件
    }
    
    // 读取数据行
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
            } else if (col.type == "VARCHAR") {
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
        cerr << "Cannot create CSV file: " << csv_file_ << endl;
        return false;
    }
    
    // 写入列名
    for (size_t i = 0; i < columns_.size(); ++i) {
        file << columns_[i].name;
        if (i < columns_.size() - 1) file << ",";
    }
    file << "\n";
    
    // 写入数据
    for (const auto& row : rows_) {
        for (size_t i = 0; i < row.size(); ++i) {
            visit([&file](auto&& arg) {
                file << arg;
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

vector<Row> Table::selectRows(
    const vector<string>& columns,
    const shared_ptr<LogicExpression>& where_clause) const {
    
    if (where_clause) {
        return filterRows(where_clause);
    }
    
    vector<Row> result;
    for (const auto& row : rows_) {
        result.push_back(row);
    }
    return result;
}

vector<Row> Table::filterRows(
    const shared_ptr<LogicExpression>& where_clause) const {
    
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

vector<Row> Table::joinTables(
    const Table& left_table,
    const Table& right_table,
    const vector<string>& columns,
    JoinType join_type,
    const JoinCondition& condition,
    const shared_ptr<LogicExpression>& where_clause) {
    
    vector<Row> result;
    
    vector<string> left_cols;
    for (const auto& col : left_table.columns_) {
        left_cols.push_back(col.name);
    }
    
    vector<string> right_cols;
    for (const auto& col : right_table.columns_) {
        right_cols.push_back(col.name);
    }
    
    int left_idx = left_table.getColumnIndex(condition.left_column);
    int right_idx = right_table.getColumnIndex(condition.right_column);
    
    if (left_idx == -1 || right_idx == -1) {
        throw runtime_error("Join column not found");
    }
    
    if (join_type != JoinType::INNER_JOIN) {
        cout << "Warning: Only INNER JOIN is currently supported" << endl;
    }
    
    // 执行INNER JOIN
    for (const auto& left_row : left_table.rows_) {
        for (const auto& right_row : right_table.rows_) {
            bool match = false;
            
            try {
                const Value& left_val = left_row[left_idx];
                const Value& right_val = right_row[right_idx];
                match = ConditionEvaluator::compare(left_val, right_val, condition.op);
            } catch (...) {
                match = false;
            }
            
            if (match) {
                vector<Value> joined_values;
                
                for (const auto& val : left_row.values()) {
                    joined_values.push_back(val);
                }
                
                for (const auto& val : right_row.values()) {
                    joined_values.push_back(val);
                }
                
                Row joined_row(joined_values);
                
                if (!where_clause) {
                    result.push_back(joined_row);
                } else {
                    vector<string> all_cols = left_cols;
                    all_cols.insert(all_cols.end(), right_cols.begin(), right_cols.end());
                    
                    if (ConditionEvaluator::evaluate(joined_row, all_cols, where_clause)) {
                        result.push_back(joined_row);
                    }
                }
            }
        }
    }
    
    return result;
}

// ==================== ConditionEvaluator 类实现 ====================
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

bool ConditionEvaluator::compareValues(const string& left, const string& right, CompareOp op) {
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

bool ConditionEvaluator::evaluate(const Row& row, 
                                 const vector<string>& column_names,
                                 const Condition& condition) {
    try {
        Value cell_value = row.getValue(condition.column, column_names);
        return compare(cell_value, condition.value, condition.op);
    } catch (...) {
        return false;
    }
}

bool ConditionEvaluator::evaluate(const Row& row,
                                 const vector<string>& column_names,
                                 const shared_ptr<LogicExpression>& expression) {
    if (!expression) return true;
    
    if (expression->isSingleCondition) {
        if (holds_alternative<Condition>(expression->left)) {
            const auto& condition = get<Condition>(expression->left);
            return evaluate(row, column_names, condition);
        }
        return false;
    }
    
    bool left_result = false;
    if (holds_alternative<Condition>(expression->left)) {
        const auto& condition = get<Condition>(expression->left);
        left_result = evaluate(row, column_names, condition);
    } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->left)) {
        const auto& left_expr = get<shared_ptr<LogicExpression>>(expression->left);
        left_result = evaluate(row, column_names, left_expr);
    }
    
    if (expression->op == LogicOp::NOT) {
        return !left_result;
    }
    
    bool right_result = false;
    if (holds_alternative<Condition>(expression->right)) {
        const auto& condition = get<Condition>(expression->right);
        right_result = evaluate(row, column_names, condition);
    } else if (holds_alternative<shared_ptr<LogicExpression>>(expression->right)) {
        const auto& right_expr = get<shared_ptr<LogicExpression>>(expression->right);
        right_result = evaluate(row, column_names, right_expr);
    }
    
    switch (expression->op) {
        case LogicOp::AND: return left_result && right_result;
        case LogicOp::OR: return left_result || right_result;
        default: return false;
    }
}

// ==================== WhereParser 类实现 ====================
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
    } else if (type == "VARCHAR") {
        if (str.front() == '\'' && str.back() == '\'') {
            return str.substr(1, str.length() - 2);
        }
        return str;
    }
    return Value{};
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

shared_ptr<LogicExpression> WhereParser::parse(const string& where_str, const vector<Column>& columns) {
    auto expression = make_shared<LogicExpression>();
    
    auto findColumnType = [&columns](const string& col_name) -> string {
        for (const auto& col : columns) {
            if (col.name == col_name) {
                return col.type;
            }
        }
        return "VARCHAR";
    };
    
    regex pattern(R"((\w+)\s*([=<>!]+)\s*('?[^']*'?|\d+\.?\d*))");
    smatch matches;
    
    if (regex_search(where_str, matches, pattern) && matches.size() >= 4) {
        string column = matches[1].str();
        string op = matches[2].str();
        string value_str = matches[3].str();
        
        string col_type = findColumnType(column);
        
        Condition condition;
        condition.column = column;
        condition.op = parseCompareOp(op);
        condition.value = parseValue(value_str, col_type);
        
        expression->isSingleCondition = true;
        expression->left = condition;
        expression->op = LogicOp::AND;
        
        return expression;
    }
    
    return nullptr;
}

// ==================== BufferPool 类实现 ====================
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

void BufferPool::saveAllTables() {
    for (auto& [name, table] : cache_) {
        table->saveToCSV();
    }
}

// ==================== MiniSQL 类实现 ====================
MiniSQL::MiniSQL() {
    buffer_pool_ = make_unique<BufferPool>();
}

void MiniSQL::createTable(const string& name, const vector<Column>& columns, const string& csv_file) {
    string csv_path = "../data/" + (csv_file.empty() ? name + ".csv" : csv_file);
    
    error_code ec;
    filesystem::create_directories("../data", ec);
    
    ofstream file(csv_path);
    if (file.is_open()) {
        for (size_t i = 0; i < columns.size(); ++i) {
            file << columns[i].name;
            if (i < columns.size() - 1) file << ",";
        }
        file << "\n";
        file.close();
    }
    
    auto table = make_shared<Table>(name, columns, csv_path);
    
    tables_[name] = table;
    buffer_pool_->putTable(name, table);
    
    cout << "Table " << name << " created successfully" << endl;
}

bool MiniSQL::insert(const string& table_name, const Row& row) {
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        string csv_path = "../data/" + table_name + ".csv";
        if (filesystem::exists(csv_path)) {
            cout << "Error: Table " << table_name << " exists but not loaded in memory" << endl;
        }
        return false;
    }
    
    table->insertRow(row);
    return true;
}

vector<Row> MiniSQL::select(
    const string& table_name,
    const vector<string>& columns,
    const shared_ptr<LogicExpression>& where_clause) {
    
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        return {};
    }
    
    // 直接执行查询，不显示优化器信息
    return table->selectRows(columns, where_clause);
}

vector<Row> MiniSQL::join(
    const string& left_table,
    const string& right_table,
    const vector<string>& columns,
    JoinType join_type,
    const JoinCondition& condition,
    const shared_ptr<LogicExpression>& where_clause) {
    
    auto left_table_ptr = buffer_pool_->getTable(left_table);
    auto right_table_ptr = buffer_pool_->getTable(right_table);
    
    if (!left_table_ptr || !right_table_ptr) {
        return {};
    }
    
    // 直接执行JOIN，不显示优化器信息
    return Table::joinTables(*left_table_ptr, *right_table_ptr, columns, join_type, condition, where_clause);
}

shared_ptr<Table> MiniSQL::getTable(const string& table_name) {
    return buffer_pool_->getTable(table_name);
}

bool MiniSQL::tableExists(const string& table_name) const {
    return tables_.find(table_name) != tables_.end();
}

void MiniSQL::saveAllTables() {
    buffer_pool_->saveAllTables();
}

vector<string> MiniSQL::getCSVFilesInDataDir() const {
    vector<string> csv_files;
    string data_dir = "../data/";
    error_code ec;
    
    if (!filesystem::exists(data_dir, ec)) {
        return csv_files;
    }
    
    try {
        for (const auto& entry : filesystem::directory_iterator(data_dir)) {
            if (entry.is_regular_file()) {
                string filename = entry.path().filename().string();
                if (filename.size() >= 4 && 
                    filename.substr(filename.size() - 4) == ".csv") {
                    csv_files.push_back(filename);
                }
            }
        }
    } catch (const filesystem::filesystem_error& e) {
        cerr << "Error scanning data directory: " << e.what() << endl;
    }
    
    return csv_files;
}

vector<string> MiniSQL::getTableNamesFromDisk() const {
    vector<string> table_names;
    auto csv_files = getCSVFilesInDataDir();
    
    for (const auto& csv_file : csv_files) {
        string table_name = csv_file.substr(0, csv_file.size() - 4);
        table_names.push_back(table_name);
    }
    
    return table_names;
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

bool MiniSQL::createTableFromJoin(
    const string& new_table_name,
    const string& left_table_name,
    const string& right_table_name,
    JoinType join_type,
    const JoinCondition& condition,
    const shared_ptr<LogicExpression>& where_clause) {
    
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
    
    if (results.empty()) {
        cout << "Warning: JOIN returned empty result set" << endl;
    }
    
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

bool MiniSQL::saveJoinAsTable(
    const string& new_table_name,
    const string& left_table_name,
    const string& right_table_name,
    const JoinCondition& condition,
    const shared_ptr<LogicExpression>& where_clause) {
    
    return createTableFromJoin(new_table_name, left_table_name, right_table_name, 
                               JoinType::INNER_JOIN, condition, where_clause);
}

bool MiniSQL::dropTable(const string& table_name) {
    bool in_memory = tables_.find(table_name) != tables_.end();
    bool on_disk = false;
    
    string csv_file = "../data/" + table_name + ".csv";
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
            cerr << "Error: Cannot delete CSV file: " << csv_file << endl;
            cerr << "Reason: ";
            
            if (ec.value() == EACCES || ec.value() == EPERM) {
                cerr << "File is open in another program (Excel, editor, etc.)" << endl;
                cerr << "Please close the file in other programs and try again." << endl;
            } else if (ec.value() == ENOENT) {
                cerr << "File not found (already deleted?)" << endl;
            } else {
                cerr << ec.message() << endl;
            }
            
            // 如果已经从内存删除，至少告诉用户这部分成功了
            if (in_memory) {
                cout << "Note: Table metadata has been removed from memory." << endl;
            }
            
            return false;
        }
    }
    
    cout << "Table '" << table_name << "' dropped successfully" << endl;
    return true;
}