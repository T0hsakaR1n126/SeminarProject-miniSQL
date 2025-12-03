// File: minisql.cpp
#include "minisql.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <memory>

// Table实现
Table::Table(std::string name, std::vector<Column> columns, std::string csv_file)
    : name_(std::move(name)), columns_(std::move(columns)), csv_file_(std::move(csv_file)) {
    if (!csv_file_.empty()) {
        loadFromCSV();
    }
}

bool Table::loadFromCSV() {
    std::ifstream file(csv_file_);
    if (!file.is_open()) {
        std::cerr << "无法打开CSV文件: " << csv_file_ << std::endl;
        return false;
    }
    
    rows_.clear();
    std::string line;
    
    // 跳过表头（假设第一行是列名）
    if (std::getline(file, line)) {
        // 可选：验证列名
    }
    
    while (std::getline(file, line)) {
        std::vector<Value> row_values;
        std::stringstream ss(line);
        std::string cell;
        size_t col_idx = 0;
        
        while (std::getline(ss, cell, ',')) {
            if (col_idx >= columns_.size()) break;
            
            const auto& col = columns_[col_idx];
            if (col.type == "INT") {
                try {
                    row_values.push_back(std::stoi(cell));
                } catch (...) {
                    row_values.push_back(0);
                }
            } else if (col.type == "DOUBLE") {
                try {
                    row_values.push_back(std::stod(cell));
                } catch (...) {
                    row_values.push_back(0.0);
                }
            } else if (col.type == "VARCHAR") {
                row_values.push_back(cell);
            }
            col_idx++;
        }
        
        if (row_values.size() == columns_.size()) {
            rows_.emplace_back(std::move(row_values));
        }
    }
    
    file.close();
    return true;
}

bool Table::saveToCSV() {
    std::ofstream file(csv_file_);
    if (!file.is_open()) {
        std::cerr << "无法创建CSV文件: " << csv_file_ << std::endl;
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
            std::visit([&file](auto&& arg) {
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
        // 可选的自动保存
        saveToCSV();
    }
}

std::vector<Row> Table::selectRows(const std::vector<std::string>& columns,
                                  const std::string& where_clause) const {
    std::vector<Row> result;
    
    // 简化版本：返回所有行
    // TODO: 实现WHERE子句过滤和列选择
    for (const auto& row : rows_) {
        result.push_back(row);
    }
    
    return result;
}

// BufferPool实现
std::shared_ptr<Table> BufferPool::getTable(const std::string& table_name) {
    auto it = cache_.find(table_name);
    if (it != cache_.end()) {
        // 更新访问顺序（LRU）
        auto order_it = std::find(access_order_.begin(), access_order_.end(), table_name);
        if (order_it != access_order_.end()) {
            access_order_.erase(order_it);
            access_order_.push_back(table_name);
        }
        return it->second;
    }
    return nullptr;
}

void BufferPool::putTable(const std::string& table_name, std::shared_ptr<Table> table) {
    if (cache_.size() >= capacity_) {
        evictLRU();
    }
    
    cache_[table_name] = table;
    access_order_.push_back(table_name);
}

void BufferPool::evictLRU() {
    if (!access_order_.empty()) {
        std::string lru_table = access_order_.front();
        access_order_.erase(access_order_.begin());
        
        auto it = cache_.find(lru_table);
        if (it != cache_.end()) {
            // 保存到CSV再移除
            it->second->saveToCSV();
            cache_.erase(it);
        }
    }
}

// SQLParser简化实现
SQLParser::SQLCommand SQLParser::parse(const std::string& sql) {
    SQLCommand cmd;
    std::stringstream ss(sql);
    std::string token;
    
    ss >> token;
    std::transform(token.begin(), token.end(), token.begin(), ::toupper);
    
    cmd.type = token;
    
    if (cmd.type == "CREATE") {
        // 解析CREATE TABLE语句
        std::string table_name;
        ss >> token; // TABLE
        ss >> table_name;
        cmd.params["table_name"] = table_name;
    } else if (cmd.type == "SELECT") {
        // 解析SELECT语句
        std::string columns_str;
        std::getline(ss, columns_str, 'F'); // 简化处理
        cmd.params["columns"] = columns_str;
    }
    
    return cmd;
}

// MiniSQL主引擎实现
MiniSQL::MiniSQL() {
    buffer_pool_ = std::make_unique<BufferPool>();
    initializeSystemTables();
}

bool MiniSQL::execute(const std::string& sql) {
    auto cmd = SQLParser::parse(sql);
    
    if (cmd.type == "CREATE") {
        // 获取参数并创建表
        auto table_name = std::get<std::string>(cmd.params["table_name"]);
        // 这里需要解析列定义，简化处理
        std::vector<Column> columns;
        // 创建表
        createTable(table_name, columns, table_name + ".csv");
        return true;
    } else if (cmd.type == "SELECT") {
        // 执行查询
        // ...
        return true;
    }
    
    return false;
}

void MiniSQL::createTable(const std::string& name,
                         const std::vector<Column>& columns,
                         const std::string& csv_file) {
    auto table = std::make_shared<Table>(name, columns, csv_file);
    tables_[name] = table;
    buffer_pool_->putTable(name, table);
    std::cout << "表 " << name << " 创建成功" << std::endl;
}

std::vector<Row> MiniSQL::select(const std::string& table_name,
                               const std::vector<std::string>& columns,
                               const std::string& where_clause) {
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        // 尝试从磁盘加载
        // ...
        return {};
    }
    
    return table->selectRows(columns, where_clause);
}

bool MiniSQL::insert(const std::string& table_name, const Row& row) {
    auto table = buffer_pool_->getTable(table_name);
    if (!table) {
        return false;
    }
    
    table->insertRow(row);
    return true;
}

void MiniSQL::initializeSystemTables() {
    // 可初始化系统表
}