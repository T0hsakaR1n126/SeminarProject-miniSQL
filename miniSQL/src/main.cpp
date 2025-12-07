#include "../include/Helper.h"
#include "../include/minisql.h"
#include <iostream>

using namespace std;

int main() {
    MiniSQL db;
    string input;
    
    cout << "========== Welcome to MiniSQL Database System ==========" << endl;
    cout << "Type 'HELP;' for available commands" << endl;
    cout << "Type 'EXIT;' to quit" << endl;
    cout << "Note: All commands MUST end with a semicolon (;)" << endl;
    
    while (true) {
        cout << "\nminisql> ";
        getline(cin, input);
        
        if (input.empty()) continue;
        if (processCommand(db, input)) {
            break;
        }
    }
    
    return 0;
}