// from Assignment 4 starter code with a few modifications/added functions

#pragma once
// LANGUAGE: CwC
#include <cstring>
#include <string>
#include <cassert>
#include "object.h"
#include "helper.h"

/** An immutable string class that wraps a character array.
 * The character array is zero terminated. The size() of the
 * String does count the terminator character. Most operations
 * work by copy, but there are exceptions (this is mostly to support
 * large strings and avoid them being copied).
 *  author: vitekj@me.com */

const char ESC = '\\'; // global escape character
const char DLM = ' '; // default deliminator between tokens
// TODO idea: create special chars array to pass to to_escape instead of creating new each call

class String : public Object {
public:
    size_t size_; // number of characters excluding terminate (\0)
    char *cstr_;  // owned; char array

    /** Build a string from a string constant */
    String(char const* cstr, size_t len) {
       size_ = len;
       cstr_ = new char[size_ + 1];
       memcpy(cstr_, cstr, size_ + 1);
       cstr_[size_] = 0; // terminate
    }
    /** Builds a string from a char*, steal must be true, we do not copy!
     *  cstr must be allocated for len+1 and must be zero terminated. */
    String(bool steal, char* cstr, size_t len) {
        assert(steal && cstr[len]==0);
        size_ = len;
        cstr_ = cstr;
    }

    String(char const* cstr) : String(cstr, strlen(cstr)) {}

    /** Build a string from another String */
    String(String & from):
        Object(from) {
        size_ = from.size_;
        cstr_ = new char[size_ + 1]; // ensure that we copy the terminator
        memcpy(cstr_, from.cstr_, size_ + 1);
    }

    /** Delete the string */
    ~String() { delete[] cstr_; }
    
    /** Return the number characters in the string (does not count the terminator) */
    size_t size() { return size_; }
    
    /** Return the raw char*. The result should not be modified or freed. */
    char* c_str() {  return cstr_; }
    
    /** Returns the character at index */
    char at(size_t index) {
        assert(index < size_);
        return cstr_[index];
    }
    
    /** Compare two strings. */
    bool equals(Object* other) {
        if (other == this) return true;
        String* x = dynamic_cast<String *>(other);
        if (x == nullptr) return false;
        if (size_ != x->size_) return false;
        return strncmp(cstr_, x->cstr_, size_) == 0;
    }

    // compares this string to the given constant string
    bool equals(const char* other) {
        return strcmp(other, cstr_) == 0; 
    }
    
    /** Deep copy of this string */
    String * clone() { return new String(*this); }

    /** This consumes cstr_, the String must be deleted next */
    char * steal() {
        char *res = cstr_;
        cstr_ = nullptr;
        return res;
    }

    /** Compute a hash for this string. */
    size_t hash_me() {
        size_t hash = 0;
        for (size_t i = 0; i < size_; ++i)
            hash = cstr_[i] + (hash << 6) + (hash << 16) - hash;
        return hash;
    }
 };

/** A string buffer builds a string from various pieces.
 *  author: jv */
class StrBuff : public Object {
public:
    char *val_; // owned; consumed by get()
    size_t capacity_;
    size_t size_;

    StrBuff() {
        capacity_ = 8;
        val_ = new char[capacity_];
        size_ = 0;
    }

    StrBuff(size_t cap) {
        val_ = new char[capacity_ = cap];
        size_ = 0;
    }

    ~StrBuff() {
        delete[] val_;
    }

    void grow_by_(size_t step) {
        if (step + size_ < capacity_) return;
        capacity_ *= 2;
        if (step + size_ >= capacity_) capacity_ += step;        
        char* oldV = val_;
        val_ = new char[capacity_];
        memcpy(val_, oldV, size_);
        delete[] oldV;
    }
    void c(const char* str) {
        size_t step = strlen(str);
        grow_by_(step);
        memcpy(val_+size_, str, step);
        size_ += step;
    }
    void c(char c) {
        grow_by_(1);
        val_[size_] = c;
        ++size_;
    }
    void c(String* s) { c(s->c_str());  }
    void c(size_t v) { c(std::to_string(v).c_str());  } // Cpp
    void c(int v) { c(std::to_string(v).c_str());  } // Cpp
    void c(float f) { c(std::to_string(f).c_str());  } // Cpp

    char* get() {
        assert(val_ != nullptr); // can be called only once
        grow_by_(1);     // ensure space for terminator
        val_[size_] = '\0'; // terminate
        return duplicate(val_);
    }

    // gets this strbuff's value without copying it
    char* no_cpy_get() {
        assert(val_ != nullptr);
        grow_by_(1);
        val_[size_] = '\0';
        char* out = val_;
        val_ = nullptr;
        return out;
    }

    // gets this strbuff's value without copying it
    // clears the buffer so it can be concatted to again starting from an empty string
    char* get_clear() {
        char* out = no_cpy_get();
        capacity_ = 8;
        val_ = new char[capacity_];
        size_ = 0;
        return out;
    }
};

// escapes necessary characters from the given string
// escapes any character that is in the given to_esc string
// @pre to_esc is null-terminated
char* add_escapes(const char* str, const char* to_esc) {
    size_t len = strlen(str);
    StrBuff* sb = new StrBuff();
    for (size_t i = 0; i < len; ++i) {
        if (strchr(to_esc, str[i]) != nullptr) sb->c(ESC); // esc if to_esc contains str[i]
        sb->c(str[i]);
    }
    char* out = sb->no_cpy_get();
    delete sb;
    return out;
}

// for tokenizing strings
// takes in a string, a character deliminator, flag for consuming escapes,
//   and a pointer to where the pointer to the rest of the string will be written
// sets given pointer to where the tokenizer left off and returns the token
// hello world - 6, &str[6]
// Note: if consume_esc == true, characters used for escaping will be consumed
//   else escaped characters will not be considered as delim, but included in token
// Note: if end of string reached, rest set to nullptr
char* next_token(char* str, char** rest, char delim, bool consume_esc) {
    StrBuff* sb = new StrBuff();
    for (int i = 0; ; ++i) {
        char cur = str[i];
        if (cur == ESC) {
            if (! consume_esc) sb->c(cur);
            sb->c(str[i+1]);
            ++i;
        } else if (cur == delim) {
            *rest = &str[i+1];
            break;
        } else if (cur == '\0') {
            // if end of string reached, rest set to nullptr
            *rest = nullptr;
            break;
        } else sb->c(cur);
    }
    char* out = sb->no_cpy_get();
    delete sb;
    return out;
}

// finds the index of the given character in the given string
// if no character is found, returns -1
// ignores escaped characters if ignore_esc is true 
//   (i.e. doesn't return index of c if c is escaped, doesn't end at '\0' if '\0' is escaped)
int index_of(char* str, char c, bool ignore_esc) {
    for (int i = 0; ; ++i) {
        char cur = str[i];
        if (ignore_esc && cur == ESC) ++i;
        else if (cur == c) return i;
        else if (cur == '\0') return -1;
    }
}
