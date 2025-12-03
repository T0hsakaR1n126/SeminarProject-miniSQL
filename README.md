C++ Smart Pointer Programming Exercise: A Simple Library Management System
---created by Zheyuan Fu, email: zheyuan.fu@uzh.ch

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


4.Compile and run the program

(1)Windows(MSYS2):
a. Run ucrt64.exe in MSYS2 folder(Yellow one).
b. Use command('cd') to Change the current working directory to the program's location.
c. Use ' g++ -o Templates Templates.cpp ' to compile the code and a Templates.exe file will be generated.('Templates' should changed to your program's name.)
d. Use './Templates.exe ' to run the program.('Templates' should changed to your program's name.)

(2)Linux(Recommend):
a. Use command('cd') to Change the current working directory to the program's location.
b. Use ' g++ -o Templates Templates.cpp ' to compile the code and a task4a file will be generated, this file do not have .exe with it.('Templates' should changed to your program's name.)
c. Use './Templates ' to run the program.('Templates' should changed to your program's name.)

(3)
a.Open Terminal from Applications/Utilities folder or search via Spotlight.
b.Use command('cd') to Change the current working directory to the program's location.
c.Compile the code using ' clang++ -o Templates Templates.cpp ' to compile the code and a task4a file will be generated, this file do not have .exe with it.('Templates' should changed to your program's name.)
d.Use './Templates ' to run the program.('Templates' should changed to your program's name.)





