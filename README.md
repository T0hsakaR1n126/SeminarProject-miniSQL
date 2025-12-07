C++ Project MiniSQL

1.C++ Development Environment Setup Guide

(1) Windows:

a. Visit MSYS2 official website: https://www.msys2.org/ to install MSYS2.
b. Run MSYS2 and a terminal for the UCRT64 environment will launch, then run command ' pacman -S mingw-w64-ucrt-x86_64-gcc ' to install C++ basic development tool.
c. Run ' g++ --version ' to verify the installation of the development tool.

Note: MSYS2 is a Linux-like development environment that runs on Windows.

(2) Linux(Recommend):

a. Run ' sudo apt update ' to update software package.
b. Run ' sudo apt install g++ ' to install C++ complier.
c. Run ' g++ --version '  to verify the installation.

(3) Mac:

a. Install Xcode Command Line Tools by running 'xcode-select --install' in Terminal.
b. Verify installation by running 'clang++ --version' in Terminal. 

2.Problem Description


2.Compile and Run the Project

(1)Windows(MSYS2):
a. Run ucrt64.exe in MSYS2 folder(Yellow one).
b. Use command('cd') to Change the current working directory to the location of file 'src'.
c. Use ' g++ -o ../bin/minisql main.cpp minisql.cpp Helper.cpp ' to compile the code and a minisql.exe file will be generated.
d. Use './../bin/minisql.exe ' to run the project.

(2)Linux(Recommend):
a. Use command('cd') to Change the current working directory to the location of file 'src'.
b. Use ' g++ -o ../bin/minisql main.cpp minisql.cpp Helper.cpp ' to compile the code and a minisql file will be generated, this file do not have .exe with it.
c. Use './../bin/minisql ' to run the project.
(3)Mac
a.Open Terminal from Applications/Utilities folder or search via Spotlight.
b.Use command('cd') to Change the current working directory to the location of file 'src'.
c.Compile the code using ' clang++ -o Templates Templates.cpp ' to compile the code and a minisql file will be generated, this file do not have .exe with it.
d.Use './../bin/minisql ' to run the project.

3.A Brief Introduction
This is a lightweight SQL database engine, called MiniSQL developed using C++. The project utilizes smart pointers and LRU mechanism for memory lifecycle management, STL containers for processing data collections, regular expressions for parsing SQL statements, and file system operations for data persistence. MiniSQL now supports standard SQL operations CREATE, INSERT, SELECT, JOIN, UPDATE, and DELETE, and has WHERE condition filtering and basic query optimization functions. It uses CSV format for data storage and loading.




