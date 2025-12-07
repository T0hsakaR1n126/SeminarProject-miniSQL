#ifndef MINISQL_H
#define MINISQL_H

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map> 
#include <variant>
#include <vector>

using namespace std;

// PartI. Define Basic Variables 
using Value = variant<int, double, string>;

struct Column {
    string name;
    string type;  
    size_t varchar_length = 0;
};

class Row {
private:
    vector<Value> values_;
    
public:
    Row() = default;
    explicit Row(vector<Value> values) : values_(move(values)) {}
    
    const Value& operator[](size_t index) const { return values_[index]; }
    Value& operator[](size_t index) { return values_[index]; }
    
    size_t size() const { return values_.size(); }
    const vector<Value>& values() const { return values_; }
    
    Value getValue(const string& column_name, const vector<string>& column_names) const;
};

enum class CompareOp {
    EQUAL,          
    NOT_EQUAL,      
    GREATER,       
    LESS,           
    GREATER_EQUAL,  
    LESS_EQUAL      
};

enum class LogicOp {
    AND,
    OR,
    NOT
};

struct Condition {
    string left_column;      
    CompareOp op;            
    Value constant_value;   
    string right_column;    
    bool is_column_comparison; 
};

struct LogicExpression {
    LogicOp op;
    variant<Condition, shared_ptr<LogicExpression>> left;
    variant<Condition, shared_ptr<LogicExpression>> right;
    bool isSingleCondition = false;
};

enum class JoinType {
    INNER_JOIN
};

struct JoinCondition {
    string left_table;
    string left_column;
    string right_table;
    string right_column;
    CompareOp op = CompareOp::EQUAL;
};

// PartII. Define Main Classes
//Define Table class include operations: CSV operation, insert, select, join and where filter.
class Table {
private:
    string name_;
    vector<Column> columns_;
    vector<Row> rows_;
    string csv_file_;
    
public:
    Table(string name, vector<Column> columns, string csv_file);
    
    //CSV operation
    bool loadFromCSV();
    bool saveToCSV();
    const string& getCsvFile() const { return csv_file_; }
    
    //INSERT operation
    void insertRow(const Row& row);
    
    //SELECT operation
    vector<Row> selectRows(const vector<string>& columns, const vector<string>& column_aliases,const shared_ptr<LogicExpression>& where_clause = nullptr) const;
    
    //condition filter
    vector<Row> filterRows(const shared_ptr<LogicExpression>& where_clause) const;
    
    void clearRows() { rows_.clear(); }
    
    //JOIN operation
    static vector<Row> joinTables(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause = nullptr);

    //DELETE operation
    int deleteRows(const shared_ptr<LogicExpression>& where_clause = nullptr);
    
    //UPDATE operation
    int updateRows(const unordered_map<string, Value>& updates, const shared_ptr<LogicExpression>& where_clause = nullptr);
    
    //Some helper functions
    int getColumnIndex(const string& column_name) const;
    const string& name() const { return name_; }
    const vector<Column>& columns() const { return columns_; }
    const vector<Row>& getAllRows() const { return rows_; }
    size_t rowCount() const { return rows_.size(); }
};

// ** Define QueryOptimizer class
class JoinOptimizer {
public:
    // This method is used to judge and choose join methods.
    static vector<Row> optimizeJoin(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type,const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause);
    
private:
    // Nested Loop Join
    static vector<Row> nestedLoopJoin(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition,const shared_ptr<LogicExpression>& where_clause);
    // Hash Join
    static vector<Row> hashJoin(const Table& left_table, const Table& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause);
};

//define a class to deal with the condition in clause.
class ConditionEvaluator {
public:
    static bool evaluate(const Row& row, const vector<string>& column_names, const Condition& condition);
    static bool evaluate(const Row& row, const vector<string>& column_names, const shared_ptr<LogicExpression>& expression);
    static bool compare(const Value& left, const Value& right, CompareOp op);
    
private:
    template<typename T>
    static bool compareValues(const T& left, const T& right, CompareOp op);
};

//define WHERE clauses parser
class WhereParser {
public:
    static shared_ptr<LogicExpression> parse(const string& where_str, const vector<Column>& columns);
    
private:
    static Value parseValue(const string& str, const string& type);
    static CompareOp parseCompareOp(const string& op_str);
    static LogicOp parseLogicOp(const string& op_str);
    static string parseColumnName(const string& column_ref, const vector<Column>& columns);
    static shared_ptr<LogicExpression> parseSingleCondition(const string& condition_str, const vector<Column>& columns);
    static shared_ptr<LogicExpression> parseExpression(const string& expr_str, const vector<Column>& columns);
    static bool validateExpression(const string& expr_str);
    static bool validateCondition(const string& cond_str);
};

// define buffer pool in which LRU mechanism will be used.
class BufferPool {
private:
    size_t capacity_;
    unordered_map<string, shared_ptr<Table>> cache_;
    vector<string> access_order_;
    
public:
    explicit BufferPool(size_t capacity = 10) : capacity_(capacity) {}
    
    shared_ptr<Table> getTable(const string& table_name);
    void putTable(const string& table_name, shared_ptr<Table> table);
    bool removeTable(const string& table_name);
    bool hasTable(const string& table_name) const {return cache_.find(table_name) != cache_.end();}
    void saveAllTables();
    
    vector<string> getAllTableNames() const {
        vector<string> names;
        for (const auto& pair : cache_) {
            names.push_back(pair.first);
        }
        return names;
    }
    
private:
    void evictLRU();
};

//Part III. Main SQL Engine
class MiniSQL {
private:
    unordered_map<string, shared_ptr<Table>> tables_;
    unique_ptr<BufferPool> buffer_pool_;
    
public:
    MiniSQL();
    
    void createTable(const string& name, const vector<Column>& columns,const string& csv_file = "");
    void saveAllTables();
    vector<string> listTables() const;
    bool dropTable(const string& table_name);
    bool insert(const string& table_name, const Row& row);
    vector<Row> select(const string& table_name, const vector<string>& columns, const vector<string>& column_aliases = {}, const shared_ptr<LogicExpression>& where_clause = nullptr);
    vector<Row> join(const string& left_table, const string& right_table, const vector<string>& columns, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause = nullptr);
    bool saveJoinAsTable(const string& new_table_name, const string& left_table_name, const string& right_table_name, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause = nullptr);
    shared_ptr<Table> getTable(const string& table_name);
    int deleteRows(const string& table_name, const shared_ptr<LogicExpression>& where_clause = nullptr);
    int updateRows(const string& table_name, const unordered_map<string, Value>& updates, const shared_ptr<LogicExpression>& where_clause = nullptr);
    
private:
    bool tableExists(const string& table_name) const;
    bool createTableFromJoin(const string& new_table_name, const string& left_table_name, const string& right_table_name, JoinType join_type, const JoinCondition& condition, const shared_ptr<LogicExpression>& where_clause = nullptr);
    vector<string> getCSVFilesInDataDir() const;
    vector<string> getTableNamesFromDisk() const;
    void loadAllTablesFromDisk();
    bool loadTableFromDisk(const string& table_name, const string& csv_path);
};

#endif