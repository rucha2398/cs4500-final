// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)
// API provided in Assignment 4

// lang::CwC

#pragma once

#include <string.h>

#include "../../util/string.h"
#include "../../util/helper.h"

/* Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
    public:
        size_t row_size_; // number of rows in this schema

        char* col_types_; // types of the columns in order
        size_t col_size_; // number of columns in this schema
        size_t col_cap_; // allocated space for col_types_ array

        /** Copying constructor */
        Schema(Schema& from) {
            col_cap_ = from.col_cap_;
            row_size_ = from.row_size_;
            col_size_ = from.col_size_;
            col_types_ = new char[col_cap_];
            
            for (size_t i = 0; i < col_size_; ++i) {
                col_types_[i] = from.col_types_[i];
            }
        }
 
        /** Create an empty schema **/
        Schema() : Schema(0, 0) {}

        // creates a schema with the given dimensions
        Schema(size_t ncols, size_t nrows) {
            if (ncols < 4) col_cap_ = 4;
            else col_cap_ = ncols;
            row_size_ = nrows;
            col_size_ = ncols;
            col_types_ = new char[col_cap_];
        }

        /** Create a schema from a string of types. A string that contains
        * characters other than those identifying the four type results in
        * undefined behavior. The argument is external, a nullptr argument is
        * undefined. **/
        Schema(const char* types) {
            int len = strlen(types);
            if (strlen(types) == 0) col_cap_ = 1;
            else col_cap_ = len;
            row_size_ = 0;
            col_size_ = len;
            
            col_types_ = new char[col_cap_];

            for (int i = 0; i < len; ++i) {
                check(is_type_(types[i]), "Invalid type");
                col_types_[i] = types[i];
            }
        }
 
        // deconstructor
        ~Schema() {
            // names are external
            delete[] col_types_;
        }
 
        // checks to see if the given character is a legal type (S, B, I, F)
        // this is a private method
        bool is_type_(char c) {
            return c == 'B' || c == 'I' || c == 'F' || c == 'S';
        }

        // grows the arrays for column data by a factor of 2
        void grow_col_() {
            col_cap_ *= 2;
            char* new_types = new char[col_cap_];
            for (size_t i = 0; i < col_size_; ++i) {
                new_types[i] = col_types_[i];
            }
            delete[] col_types_;
            col_types_ = new_types;
        }
        
        /** Add a column of the given type */
        // error on invalid type
        void add_column(char typ) {
            check(is_type_(typ), "Invalid type");
            
            if (col_size_ == col_cap_) grow_col_();
            col_types_[col_size_] = typ;
            ++col_size_;
        }
         
        /** Add a row with a name (possibly nullptr), name is external.  Names are
         *  expectd to be unique, duplicates result in undefined behavior. */
        // error on duplicate name
        void add_row() {
            ++row_size_;
        }
        
        /** Return type of column at idx. An idx >= width is undefined. */
        // if index is out of bounds, the program exits
        char col_type(size_t idx) {
            check(idx < col_size_, "Index out of bounds");
            return col_types_[idx];
        }
        
        /** The number of columns */
        size_t width() {
            return col_size_;
        }
         
        /** The number of rows */
        size_t length() {
            return row_size_;
        }
};
