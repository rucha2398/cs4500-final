// Based on helper code from Assignment 4 with some new functions
#pragma once
//lang::Cpp

#include <cstdlib>
#include <cstring>
#include <iostream>

// Error with message
void error(const char* msg) {
    fprintf(stderr, "%s: ", msg);
    perror("");
    exit(-1);
}
// checks if the given boolean is true (like assert) and puts error message if false
void check(bool b, const char* msg) {
    if (!b) error(msg);
}

// String equals
bool streq(const char* str1, const char* str2) {
    if (str1 == nullptr || str2 == nullptr) return str1 == str2;
    else return strcmp(str1, str2) == 0;
}

//testing float equality
bool float_eq(float f1, float f2) {
    return (f1 - f2) < 0.01 || (f1 - f2) > -0.01;
}

// this is a helper function since C++ does not handle negative mod correctly
// https://stackoverflow.com/questions/38251229/modulo-with-a-negative-integer-in-c
// found the solution in the above link ^
int mod(int n1, int n2) {
    return (n1 % n2 + n2) % n2;
}

// Printing functions
void p(char* c) { std::cout << c; }
void p(bool c) { std::cout << c; }
void p(float c) { std::cout << c; }  
void p(int i) { std::cout << i; }
void p(size_t i) { std::cout << i; }
void p(const char* c) { std::cout << c; }
void p(char c) { std::cout << c; }
void pln() { std::cout << "\n"; }
void pln(int i) { std::cout << i << "\n"; }
void pln(char* c) { std::cout << c << "\n"; }
void pln(bool c) { std::cout << c << "\n"; }  
void pln(char c) { std::cout << c << "\n"; }
void pln(float x) { std::cout << x << "\n"; }
void pln(size_t x) { std::cout << x << "\n"; }
void pln(const char* c) { std::cout << c << "\n"; }

// Copying strings
char* duplicate(const char* s) {
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
}
char* duplicate(char* s) {
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
}



// Function to terminate execution with a message
void exit_if_not(bool b, char* c) {
    if (b) return;
    p("Exit message: ");
    pln(c);
    exit(-1);
}

// Definitely fail
//  void FAIL() {
void myfail(){
    pln("Failing");
    exit(1);
}

// Some utilities for lightweight testing
void OK(const char* m) { pln(m); }
void t_true(bool p) { if (!p) myfail(); }
void t_false(bool p) { if (p) myfail(); }
