// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)
// API provided in Assignment 4
// lang::CwC

#pragma once

#include <cstdarg>

#include "../../util/object.h"
#include "../../util/string.h"
#include "../../util/helper.h"

class BoolColumn;
class IntColumn;
class FloatColumn;
class StringColumn;

/* Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column : public Object {
    public:
        // default constructor
        Column() : Object() {}

        /** Type converters: Return same column under its actual type, or
        *  nullptr if of the wrong type.  */
        virtual IntColumn* as_int() { return nullptr; }
        virtual BoolColumn*  as_bool() { return nullptr; }
        virtual FloatColumn* as_float() { return nullptr; }
        virtual StringColumn* as_string() { return nullptr; }
 
        /** Type appropriate push_back methods. Calling the wrong method is
        * undefined behavior. **/
        virtual void push_back(int val) { check(false, "Can't push_back from parent Column"); }
        virtual void push_back(bool val) { check(false, "Can't push_back from parent Column"); }
        virtual void push_back(float val) { check(false, "Can't push_back from parent Column"); }
        virtual void push_back(String* val) { check(false, "Can't push_back from parent Column"); }
 
        /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
        virtual char get_type() { 
            check(false, "Can't call get_type from parent Column"); 
            return 'z';
        }
        
        // returns 0 since it cannot calculate size
        // should be overwritten in child classes
        virtual size_t size() { return 0; } 

        // serializes this Column
        virtual char* serialize() { 
            check(false, "Serialize called on parent Column class"); 
            return nullptr;
        }

        // deserializes this Column
        static Column* deserialize(char* m, char type, size_t size);
};

/* BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
    public:

        bool* vals_;
        size_t cap_;
        size_t size_;

        // initializes this boolean column as an empty column
        BoolColumn() {
            size_ = 0;
            cap_ = 4;
            vals_ = new bool[cap_];
        }
        
        // creates a new boolean column with the given size
        // capacity will be maxed with 1 if given 0
        // all values initialized to 0
        BoolColumn(size_t size) {
            if (size == 0) cap_ = 1;
            else cap_ = size;
            vals_ = new bool[cap_];
            memset(vals_, 0, sizeof(bool) * cap_);
            size_ = size;
        }

        // constructor takes in n to represent the number of arguments and
        // then a variable number of bools to fill the column
        // behavior is undefined if the given arguments are not bools
        BoolColumn(int n, ...) {
            if (n == 0) cap_ = 1;
            else cap_ = n;
            va_list args;
            va_start(args, n);
            
            vals_ = new bool[cap_];
            for (int i = 0; i < n; ++i) {
                int integer = va_arg(args, int);
                if (integer == 0) vals_[i] = 0;
                else if (integer == 1) vals_[i] = 1;
                else check(false, "Invalid boolean"); 
            }
            va_end(args);
            size_ = n;
        }

        // deconstructor for this boolean column
        ~BoolColumn() {
            delete[] vals_;
        }

        /** Returns the number of elements in the column. */
        size_t size() { return size_; }
 
        // gets the element at the given index in this column
        bool get(size_t idx) {
            check(idx < size_, "Index out of bounds");
            return vals_[idx];
        }

        // returns this column since it is already a BoolColumn type
        BoolColumn* as_bool() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, bool val) {
            check(idx < size_, "Index of out bounds");
            vals_[idx] = val;
        }

        // this is a private method that doubles the allocate space for this column
        // @post maintains element indexing and size, capacity doubles
        void grow_() {
            cap_ *= 2;
            bool* new_vals = new bool[cap_];
            for (size_t i = 0; i < size_; ++i) {
                new_vals[i] = vals_[i];
            }
            delete[] vals_;
            vals_ = new_vals;
        }

        // pushes the given boolean into this column
        void push_back(bool val) {
            if (size_ == cap_) {
                grow_();
            }
            vals_[size_] = val;
            ++size_;
        }

        // returns I since this column is an boolean column
        char get_type() { return 'B'; }

        // serializes this BoolColumn into the following format:
        // [<bool0> <bool1> <bool2>]
        char* serialize() {
            StrBuff* sb = new StrBuff();
            sb->c('[');
            
            for (size_t i = 0; i < size_; ++i) {
                if (i != 0) sb->c(DLM);
                sb->c(vals_[i]);
            }

            sb->c(']');

            char* out = sb->no_cpy_get();
            delete sb;
            return out;
        }

        // deserializes the given string into a BoolColumn of the given size
        static BoolColumn* deserialize(char* m, size_t size) {
            char* rest = nullptr;
            // skip to inside brackets
            char* tok;
            delete[] next_token(m, &rest, '[', false);
            
            BoolColumn* out = new BoolColumn(size);            
            for (size_t i = 0; i < size; ++i) {
                if (i < size - 1) tok = next_token(rest, &rest, DLM, false);
                else tok = next_token(rest, &rest, ']', false);
                out->set(i, atoi(tok));
                delete[] tok;
            }

            return out;
        }
};

/* IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
    public:

        int* vals_;
        size_t cap_;
        size_t size_;

        // initializes this integer column as an empty column
        IntColumn() {
            size_ = 0;
            cap_ = 4;
            vals_ = new int[cap_]; 
        }
        
        // creates a new integer column with the given size
        // capacity will be maxed with 1 if given 0
        // all values initialized to 0
        IntColumn(size_t size) {
            if (size == 0) cap_ = 1;
            else cap_ = size;
            vals_ = new int[cap_];
            memset(vals_, 0, sizeof(int) * cap_);
            size_ = size;
        }

        // constructor takes in n to represent the number of arguments and
        // then a variable number of ints to fill the column
        // behavior is undefined if the given arguments are not ints
        IntColumn(int n, ...) {
            if (n == 0) cap_ = 1;
            else cap_ = n;
            
            va_list args;
            va_start(args, n);

            vals_ = new int[cap_];
            for (int i = 0; i < n; ++i) {
                vals_[i] = va_arg(args, int);
            }

            va_end(args);
            size_ = n;
        }

        // deconstructor for this integer column
        ~IntColumn() {
            delete[] vals_;
        }

        /** Returns the number of elements in the column. */
        size_t size() { return size_; }
 
        // gets the element at the given index in this column
        int get(size_t idx) {
            check(idx < size_, "Index out of bounds");
            return vals_[idx];
        }

        // returns this coluumn since it is already an IntColumn type
        IntColumn* as_int() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, int val) {
            check(idx < size_, "Index out of bounds");
            vals_[idx] = val;
        }

        // this is a private method that doubles the allocate space for this column
        // @post maintains element indexing and size, capacity doubles
        void grow_() {
            cap_ *= 2;
            int* new_vals = new int[cap_];
            for (size_t i = 0; i < size_; ++i) {
                new_vals[i] = vals_[i];
            }
            delete[] vals_;
            vals_ = new_vals;
        }

        // pushes the given integer into this column
        void push_back(int val) {
            if (size_ == cap_) {
                grow_();
            }
            vals_[size_] = val;
            ++size_;
        }

        // returns I since this column is an integer column
        char get_type() { return 'I'; }

        // serializes this IntColumn into the following format:
        // [<int0> <int1> <int2>]
        char* serialize() {
            StrBuff* sb = new StrBuff();
            sb->c('[');

            for (size_t i = 0; i < size_; ++i) {
                if (i != 0) sb->c(DLM);
                sb->c(vals_[i]);
            }

            sb->c(']');

            char* out = sb->no_cpy_get();
            delete sb;
            return out;
        }

        // deserializes the given string into a IntColumn of the given size
        static IntColumn* deserialize(char* m, size_t size) {
            char* rest = nullptr;
            // skip to inside brackets
            char* tok;
            delete[] next_token(m, &rest, '[', false);
            
            IntColumn* out = new IntColumn(size);
            for (size_t i = 0; i < size; ++i) {
                if (i < size - 1) tok = next_token(rest, &rest, DLM, false);
                else tok = next_token(rest, &rest, ']', false);
                out->set(i, atoi(tok));
                delete[] tok;
            }

            return out;
        }

};

/* FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column {
    public:

        float* vals_;
        size_t size_;
        size_t cap_;

        // initializes this float column as an empty column
        FloatColumn() {
            size_ = 0;
            cap_ = 4;
            vals_ = new float[cap_];
        }
        
        // creates a new float column with the given size
        // capacity will be maxed with 1 if given 0
        // all values initalized to 0
        FloatColumn(size_t size) {
            if (size == 0) cap_ = 1;
            else cap_ = size;
            vals_ = new float[cap_];
            memset(vals_, 0, sizeof(float) * cap_);
            size_ = size;
        }

        // constructor takes in n to represent the number of arguments and
        // then a variable number of floats to fill the column
        // behavior is undefined if the given arguments are not floats
        FloatColumn(int n, ...) {
            if (n == 0) cap_ = 1;
            else cap_ = n;
            
            va_list args;
            va_start(args, n);

            vals_ = new float[cap_];
            for (int i = 0; i < n; ++i) {
                // needs to be double because va_arg doesn't deal with floats
                double d = va_arg(args, double);
                // need to c style cast because reinterpret cast doesn't compile
                vals_[i] = (float)d;
            }

            va_end(args);
            size_ = n;
        }

        // deconstructor for this float column
        ~FloatColumn() {
            delete[] vals_;
        }

        /** Returns the number of elements in the column. */
        size_t size() { return size_; }
        
        // gets the element at the given index in this column
        float get(size_t idx) {
            check(idx < size_, "Index out of bounds");
            return vals_[idx];
        }

        // returns this coluumn since it is already an FloatColumn type
        FloatColumn* as_float() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, float val) {
            check(idx < size_, "Index out of bounds");
            vals_[idx] = val;
        }

        // this is a private method that doubles the allocate space for this column
        // @post maintains element indexing and size, capacity doubles
        void grow_() {
            cap_ *= 2;
            float* new_vals = new float[cap_];
            for (size_t i = 0; i < size_; ++i) {
                new_vals[i] = vals_[i];
            }
            delete[] vals_;
            vals_ = new_vals;
        }

        // pushes the given float into this column
        void push_back(float val) {
            if (size_ == cap_) {
                grow_();
            }
            vals_[size_] = val;
            ++size_;
        }

        // returns I since this column is an float column
        char get_type() { return 'F'; }

        // serializes this FloatColumn into the following format:
        // [<float0> <float1> <float2>]
        char* serialize() {
            StrBuff* sb = new StrBuff();
            sb->c('[');

            for (size_t i = 0; i < size_; ++i) {
                if (i != 0) sb->c(DLM);
                sb->c(vals_[i]);
            }

            sb->c(']');

            char* out = sb->no_cpy_get();
            delete sb;
            return out;
        }

        // deserializes the given string into a FloatColumn of the given size
        static FloatColumn* deserialize(char* m, size_t size) {
            char* rest = nullptr;
            // skip to inside brackets
            char* tok;
            delete[] next_token(m, &rest, '[', false);

            FloatColumn* out = new FloatColumn(size);
            for (size_t i = 0; i < size; ++i) {
                if (i < size - 1) tok = next_token(rest, &rest, DLM, false);
                else tok = next_token(rest, &rest, ']', false);
                out->set(i, atof(tok));
                delete[] tok;
            }
            return out;
        }

};

/* StringColumn::
 * Holds String values.
 * Strings are EXTERNAL
 */
class StringColumn : public Column {
    public:

        String** vals_; // these string values are OWNED
        size_t size_;
        size_t cap_;

        // initializes this String column as an empty column
        StringColumn() {
            size_ = 0;
            cap_ = 4;
            vals_ = new String*[cap_];
        }
        
        // creates a new String column with the given size
        // capacity will be maxed with 1 if given 0
        // all values initialized to nullptr
        StringColumn(size_t size) {
            if (size == 0) cap_ = 1;
            else cap_ = size;
            vals_ = new String*[cap_];
            memset(vals_, 0, sizeof(String*) * cap_);
            size_ = size;
        }

        // constructor takes in n to represent the number of arguments and
        // then a variable number of Strings to fill the column
        // behavior is undefined if the given arguments are not Strings
        StringColumn(int n, ...) {
            if (n == 0) cap_ = 1;
            else cap_ = n;
            va_list args;
            va_start(args, n);

            vals_ = new String*[cap_];
            for (int i = 0; i < n; ++i) {
                vals_[i] = va_arg(args, String*);
            }

            va_end(args);
            size_ = n;
        }

        // deconstructor for this String column
        // DOES delete strings since they are NOT external
        ~StringColumn() {
            for (size_t i = 0; i < size_; ++i) delete vals_[i];
            delete[] vals_;
        }

        /** Returns the number of elements in the column. */
        size_t size() { return size_; }
 
        // gets the element at the given index in this column
        String* get(size_t idx) {
            check(idx < size_, "Index out of bounds");
            return vals_[idx];
        }

        // returns this column since it is already an StringColumn type
        StringColumn* as_string() { return this; }

        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, String* val) {
            check(idx < size_, "Index out of bounds");
            vals_[idx] = val;
        }

        // this is a private method that doubles the allocate space for this column
        // @post maintains element indexing and size, capacity doubles
        void grow_() {
            cap_ *= 2;
            String** new_vals = new String*[cap_];
            for (size_t i = 0; i < size_; ++i) {
                new_vals[i] = vals_[i];
            }
            delete[] vals_;
            vals_ = new_vals;
        }

        // pushes the given string into this column
        void push_back(String* val) {
            if (size_ == cap_) {
                grow_();
            }
            vals_[size_] = val;
            ++size_;
        }

        // returns I since this column is an String column
        char get_type() { return 'S'; }

        // serializes this StringColumn into the following format:
        // [<str0> <str1> <str2>]
        char* serialize() {
            StrBuff* sb = new StrBuff();
            sb->c('[');

            for (size_t i = 0; i < size_; ++i) {
                if (i != 0) sb->c(DLM);
                char* tmp = duplicate(vals_[i]->c_str());
                char to_esc[] = {ESC, DLM, '}', ']', '\n'};
                char* tmp2 = add_escapes(tmp, to_esc);
                delete[] tmp;
                sb->c(tmp2);
                delete[] tmp2;
            }

            sb->c(']');

            char* out = sb->no_cpy_get();
            delete sb;
            return out;
        }

        // deserializes the given string into a StringColumn of the given size
        static StringColumn* deserialize(char* m, size_t size) {
            char* rest = nullptr;
            // skip to inside brackets
            char* tok;
            delete[] next_token(m, &rest, '[', false);

            StringColumn* out = new StringColumn(size);
            for (size_t i = 0; i < size; ++i) {
                if (i < size - 1) tok = next_token(rest, &rest, DLM, true);
                else tok = next_token(rest, &rest, ']', true);
                out->set(i, new String(tok));
                delete[] tok;
            }

            return out;
        }

};

// deserializes the given string into a column
Column* Column::deserialize(char* m, char type, size_t size) {
    if (type == 'B') return BoolColumn::deserialize(m, size);
    else if (type == 'I') return IntColumn::deserialize(m, size);
    else if (type == 'F') return FloatColumn::deserialize(m, size);
    else if (type == 'S') return StringColumn::deserialize(m, size);
    else check(false, "Invalid Column type");
    return nullptr;
}
