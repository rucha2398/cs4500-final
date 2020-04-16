// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)
// API provided in Assignment 4

// lang::CwC

#pragma once

#include <assert.h>
#include "schema.h"
#include "../../util/string.h"
#include "../../util/object.h"
#include "fielder.h"

/* Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
    public:
        
        bool* bools_;
        int* ints_;
        float* floats_;
        String** strs_;

        size_t idx_; // index of this row

        Schema* schema_;

        // the sets do not change the data in the data frame, it is only for information purposes
        /** Build a row following a schema. */
        // given schema is duped
        Row(Schema& scm) {
            schema_ = new Schema(scm);
            bools_ = new bool[scm.width()];
            ints_ = new int[scm.width()];
            floats_ = new float[scm.width()];
            strs_ = new String*[scm.width()];
            idx_ = -1;
        }

        // deconstructor for Row
        // deletes local schema since it was copied in constructor
        ~Row() {
            delete[] bools_;
            delete[] ints_;
            delete[] floats_;
            delete[] strs_;
            delete schema_;
        }
 
        /** Setters: set the given column with the given value. Setting a column with
        * a value of the wrong type is undefined. */
        // if the column is not a boolean column, it will call the integer set()
        void set(size_t col, bool val) {
            // schema should error if invalid index
            if (col_type(col) == 'B') {
                bools_[col] = val;
            } else set(col, (int)val);
        }

        // if the column is not an integer column, it will call the float set()
        void set(size_t col, int val) {
            if (col_type(col) == 'I') {
                ints_[col] = val;
            } else set(col, (float)val);
        }

        // if the column is not a float column, the program will exit (no escalation)
        void set(size_t col, float val) {
            if (col_type(col) == 'F') {
                floats_[col] = val;
            } else assert(false);
        }
        
        /** The string is external. */
        // if the column is not a String column, the program will exit (no escalation)
        void set(size_t col, String* val) {
            if (col_type(col) == 'S') {
                strs_[col] = val;
            } else assert(false);
        }
 
        /** Set/get the index of this row (ie. its position in the dataframe. This is
        *  only used for informational purposes, unused otherwise */
        void set_idx(size_t idx) {
            assert(idx < schema_->length());
            idx_ = idx;
        }
        size_t get_idx() { return idx_; }
 
        /** Getters: get the value at the given column. If the column is not
        * of the requested type, the result is undefined. */
        bool get_bool(size_t col) {
            if (schema_->col_type(col) == 'B') {
                return bools_[col];
            } else check(false, "Incorrect type");
            return 0;
        }
        int get_int(size_t col) {
            if (schema_->col_type(col) == 'I') {
                return ints_[col];
            } else check(false, "Incorrect type");
            return 0;
        }
        float get_float(size_t col) {
            if (schema_->col_type(col) == 'F') {
                return floats_[col];
            } else check(false, "Incorrect type");
            return 0;
        }
        String* get_string(size_t col) {
            if (schema_->col_type(col) == 'S') {
                return strs_[col];
            } else check(false, "Incorrect type");
            return nullptr;
        }
 
        /** Number of fields in the row. */
        size_t width() {
            return schema_->width();
        }
 
        /** Type of the field at the given position. An idx >= width is  undefined. */
        char col_type(size_t idx) {
            return schema_->col_type(idx);
        }
 
        /** Given a Fielder, visit every field of this row. The first argument is
        * index of the row in the dataframe.
        * Calling this method before the row's fields have been set is undefined. */
        void visit(size_t idx, Fielder& f) {
            // assert that the fielder is trying to visit the expected row
            check(idx == idx_, "Invalid index"); 
            f.start(idx_);
            for (size_t i = 0; i < width(); ++i) {
                if (col_type(i) == 'B') f.accept(get_bool(i));
                else if (col_type(i) == 'I') f.accept(get_int(i));
                else if (col_type(i) == 'F') f.accept(get_float(i));
                else if (col_type(i) == 'S') f.accept(get_string(i));
            }
            f.done();    
        }
};
