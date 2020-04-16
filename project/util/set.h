// Authors: Zoe Corning (corning.z@husky.neu.edu) & Rucha Khanolkar (khanolkar.r@husky.neu.edu)
#pragma once

#include "helper.h"
#include "object.h"
#include "string.h"

// This is a set of integers which uses bools to determine whether an integer is/isn't in the set 
// if the boolean at index of the int is true, then the int is in the set
class Set : public Object {
    public:
        bool* set_; // array of booleans representing int set
        size_t max_; // maximum integer that can be in this set
        size_t size_; // number of elements in this set

        // creates a set that can contain integers from 0 up to max (inclusive)
        // set starts empty
        Set(size_t max) : Object() {
            set_ = new bool[max+1];
            memset(set_, 0, sizeof(bool) * (max + 1));
            max_ = max;
            size_ = 0;
        }

        // deconstructor
        ~Set() {
            delete[] set_;
        }

        // returns the size of this set (the number of elements in this set)
        size_t size() { return size_; }

        // adds the given number to this set
        void add(size_t n) {
            check(n <= max_, "Out of bounds");
            if (!set_[n]) ++size_;
            set_[n] = true;
        }

        // removes the given number from this set
        void remove(size_t n) {
            check(n <= max_, "Out of bounds");
            if (set_[n]) --size_;
            set_[n] = false; 
        }

        // returns true if the given element is in the set
        bool contains(size_t n) {
            if (n > max_) return false;
            else return set_[n];
        }

        // returns an array of the elements in this set
        // size of array = this->size()
        int* elements() {
            int* out = new int[size_];
            int out_i = 0; // index into out array
            for (size_t set_i = 0; set_i < max_; ++set_i) {
                if (set_[set_i]) {
                    out[out_i] = set_i;
                    ++out_i;
                }
            }
            return out;
        }

        // clears this set of all elements
        void clear() {
            memset(set_, 0, sizeof(bool) * (max_ + 1));
            size_ = 0;
        }

        // serializes this set into the following format:
        // <max> <size> {<n_0> <n_1> <n_2> ... <n_n>}
        // where n_i is an integer in this set
        char* serialize() {
            StrBuff* sb = new StrBuff();
            sb->c(max_);
            sb->c(DLM);
            sb->c(size_);
            sb->c(" {");
            bool first = true; // set to false once the first element is found (for spacing)
            for (size_t i = 0; i <= max_; ++i) {
                if (set_[i]) {
                    if (first) first = false;
                    else sb->c(DLM);
                    sb->c(i);
                }
            }

            sb->c('}');

            char* out = sb->no_cpy_get();
            delete sb;
            return out;
        }

        // deserializes the given string into a Set
        static Set* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            size_t max = atoi(tok);
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            size_t size = atoi(tok);
            delete[] tok;
            
            // skip to inside of brackets
            delete[] next_token(rest, &rest, '{', false);

            Set* out = new Set(max);
            for (size_t i = 0; i < size; ++i) {
                if (i == size - 1) tok = next_token(rest, &rest, '}', false);
                else tok = next_token(rest, &rest, DLM, false);
                out->add(atoi(tok));
                delete[] tok;
            }

            return out;
        }
};
