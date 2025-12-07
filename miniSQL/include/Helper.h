#ifndef HELPER_H
#define HELPER_H

#include "minisql.h"
#include <string>
#include <vector>
#include <memory>

using namespace std;

// helperfunctions declaration
//Query parser related helper functions.
string trim(const string& str);
vector<string> split(const string& str, char delimiter);
char toUpperChar(char c);
size_t findOuterOperator(const string& expr, const string& op);
vector<Column> parseColumnDefinitions(const string& columns_str);
shared_ptr<LogicExpression> parseWhereClause(const string& where_str, const shared_ptr<Table>& table);
shared_ptr<LogicExpression> parseJoinWhereClause(const string& where_str, const shared_ptr<Table>& left_table, const shared_ptr<Table>& right_table);
JoinCondition parseJoinCondition(const string& join_str);
unordered_map<string, Value> parseUpdateSet(const string& set_clause, const shared_ptr<Table>& table);

//Query prehandle helper functions
bool processCommand(MiniSQL& db, const string& input);
void handleCreateTable(MiniSQL& db, const string& input);
void handleInsert(MiniSQL& db, const string& input);
void handleSimpleSelect(MiniSQL& db, const string& input);
void handleJoinSelect(MiniSQL& db, const string& input, bool has_save_as, const string& save_table_name);
void handleDropTable(MiniSQL& db, const string& input);
void handleShowTables(MiniSQL& db);
void handleDelete(MiniSQL& db, const string& input);
void handleUpdate(MiniSQL& db, const string& input);

// Interface helper functions
void displayResults(const vector<Row>& results, const vector<Column>& columns);
void showHelp();

#endif