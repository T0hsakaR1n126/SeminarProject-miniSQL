// File: minisql.h
#ifndef MINISQL_H
#define MINISQL_H

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <variant>

// 数据类型定义
using Value = std::variant<int, double, std::string>;

// 元组（行）类
class Row {
private:
    std::vector<Value> values_;
    
public:
    Row() = default;
    explicit Row(std::vector<Value> values) : values_(std::move(values)) {}
    
    const Value& operator[](size_t index) const { return values_[index]; }
    Value& operator[](size_t index) { return values_[index]; }
    
    size_t size() const { return values_.size(); }
    const std::vector<Value>& values() const { return values_; }
};

// 表元数据
struct Column {
    std::string name;
    std::string type;  // "INT", "DOUBLE", "VARCHAR"
    size_t varchar_length = 0;
};

// 表类
class Table {
private:
    std::string name_;
    std::vector<Column> columns_;
    std::vector<Row> rows_;
    std::string csv_file_;
    
public:
    Table(std::string name, std::vector<Column> columns, std::string csv_file);
    
    // 加载/保存CSV
    bool loadFromCSV();
    bool saveToCSV();
    
    // 数据操作
    void insertRow(const Row& row);
    std::vector<Row> selectRows(const std::vector<std::string>& columns,
                                const std::string& where_clause = "") const;
    
    const std::string& name() const { return name_; }
    const std::vector<Column>& columns() const { return columns_; }
    size_t rowCount() const { return rows_.size(); }
};

// SQL解析器
class SQLParser {
public:
    struct SQLCommand {
        std::string type;  // "CREATE", "SELECT", "INSERT", "JOIN"
        std::unordered_map<std::string, std::variant<std::string, std::vector<std::string>>> params;
    };
    
    static SQLCommand parse(const std::string& sql);
};

// 查询优化器
class QueryOptimizer {
public:
    struct QueryPlan {
        std::string operation;
        std::shared_ptr<Table> table;
        std::string filter_condition;
        // 更多优化参数...
    };
    
    static QueryPlan optimize(const SQLParser::SQLCommand& cmd);
};

// 缓冲池
class BufferPool {
private:
    size_t capacity_;
    std::unordered_map<std::string, std::shared_ptr<Table>> cache_;
    std::vector<std::string> access_order_;
    
public:
    explicit BufferPool(size_t capacity = 10) : capacity_(capacity) {}
    
    std::shared_ptr<Table> getTable(const std::string& table_name);
    void putTable(const std::string& table_name, std::shared_ptr<Table> table);
    
private:
    void evictLRU();
};

// MiniSQL主引擎
class MiniSQL {
private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::unique_ptr<BufferPool> buffer_pool_;
    
public:
    MiniSQL();
    
    // 核心API
    bool execute(const std::string& sql);
    void createTable(const std::string& name, 
                     const std::vector<Column>& columns,
                     const std::string& csv_file = "");
    
    std::vector<Row> select(const std::string& table_name,
                           const std::vector<std::string>& columns,
                           const std::string& where_clause = "");
    
    bool insert(const std::string& table_name, const Row& row);
    
private:
    void initializeSystemTables();
};

#endif // MINISQL_H