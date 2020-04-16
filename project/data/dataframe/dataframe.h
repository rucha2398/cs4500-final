// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)
// API provided in Assignment 4

// lang::CwC

#pragma once

#include <thread>
#include "../../util/object.h"
#include "../../util/string.h"
#include "../../util/helper.h"
#include "schema.h"
#include "row.h"
#include "fielder.h"
#include "rower.h"
#include "column.h"

class DataFrame;

// this class prints out a single field in SoR format
// used in the DataFrame's print method
class FieldPrinter : public Fielder {
    public:
        // does nothing, doesn't need to print anything at start
        void start(size_t r) { return; }

        // prints out the given bool
        void accept(bool b) {
            printf("<%d>", b);
        }

        // prints out the given int
        void accept(int i) {
            printf("<%d>", i);
        }

        // prints out the given float
        void accept(float f) {
            printf("<%f>", f);
        }

        // prints out the given string
        void accept(String* s) {
            if (s == nullptr) printf("<>");
            else printf("<\"%s\">", s->cstr_);
        }
        
        // doesn't need to print anything at the end
        void done() { return; }
};

// this class prints out a single row in SoR format
// used in the DataFrame's print method
class RowPrinter : public Rower {
    public:
        // prints out the given row
        bool accept(Row& r) {
            FieldPrinter* fp = new FieldPrinter();
            r.visit(r.get_idx(), *fp);
            delete fp;
            return true;
        }

        // cannot be parallelized because it needs to prsize_t in order
        // so does not override join_delete()
};

class PMapData : public Object {
    public:
        DataFrame* df_;
        Rower* r_;
        size_t start_;
        size_t end_;

        PMapData(DataFrame* df, Rower* r, size_t start, size_t end) {
            df_ = df;
            r_ = r;
            start_ = start;
            end_ = end;
        }
};

/*
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object {
    public:
        Schema* s_; // internal
        Column** cols_; // columns are copies of given columns so these are internal
        size_t size_;
        size_t cap_;

        Row* row_; // used so you don't have to reallocate memory
 
        /** Create a data frame with the same columns as the given df but with no rows or row names */
        DataFrame(DataFrame& df) {
            s_ = new Schema();
            // adds all the columns, but not the rows
            for (size_t i = 0; i < df.ncols(); ++i) {
                s_->add_column(df.s_->col_type(i));
            }
            size_ = df.size_;
            cap_ = df.cap_;
            cols_ = new Column*[cap_];

            // copies copy the other data frame's columns so it can have an internal representation
            for (size_t i = 0; i < size_; ++i) {
                cols_[i] = copy_column_(df.cols_[i]);
            }

            row_ = nullptr;
        }
 
        /** Create a data frame from a schema and columns. All columns are created
        * empty. */
        DataFrame(Schema& schema) {
            s_ = new Schema(schema);
            if (schema.width() == 0) cap_ = 1;
            else cap_ = schema.width();
            size_ = schema.width();
            cols_ = new Column*[cap_];

            // allocate/initialize all the columns
            for (size_t i = 0; i < schema.width(); ++i) {
                cols_[i] = create_column_(schema.col_type(i), schema.length());
            }

            row_ = nullptr;
        }

        ~DataFrame() {
            delete s_;
            // deletes every column since they are stored internally
            for (size_t i = 0; i < size_; ++i) {
                delete cols_[i];
            }
            delete[] cols_;
            delete row_;
        }

        /** Returns the dataframe's schema. Modifying the schema after a dataframe
        * has been created in undefined. */
        Schema& get_schema() {
            return *s_;
        }
        
        //this is a private method that grows the array of column data by a factor of 2
        void grow_() {
            cap_ *= 2;
            Column** new_cols = new Column*[cap_];
            for(size_t i = 0; i< size_; ++i) {
                new_cols[i] = cols_[i];
            }
            delete[] cols_;
            cols_ = new_cols;

        }

        // creates a column of the given type with the given capacity
        // this is a private method
        Column* create_column_(char type, size_t capacity) {
            if (type == 'B') return new BoolColumn(capacity);
            else if (type == 'I') return new IntColumn(capacity);
            else if (type == 'F') return new FloatColumn(capacity);
            else if (type == 'S') return  new StringColumn(capacity);
            else check(false, "Invalid type");
            return nullptr;
        }

        // returns the column at the given index
        // also makes sure the given index is in bounds
        Column* get_col_(size_t idx) {
            check(idx < size_, "Index out of bounds");
            return cols_[idx];
        }

        // duplicates the given column so its representation can be internal
        // this is a private method
        Column* copy_column_(Column* c) {
            char type = c->get_type();
            Column* out = create_column_(type, c->size());
            for (size_t i = 0; i < c->size(); ++i) {
                if (type == 'B') {
                    out->as_bool()->set(i, c->as_bool()->get(i));
                } else if (type == 'I') {
                    out->as_int()->set(i, c->as_int()->get(i));
                } else if (type == 'F') {
                    out->as_float()->set(i, c->as_float()->get(i));
                } else if (type == 'S') {
                    out->as_string()->set(i, c->as_string()->get(i));
                } else check(false, "Invalid type"); 
            }
            return out;
        }

        /** Adds a column this dataframe, updates the schema, the new column
        * is external, and appears as the last column of the dataframe, the
        * name is optional and external. A nullptr colum is undefined. */
        void add_column(Column* col) {
            check(col != nullptr, "Null column");
            check(col->size() == s_->length(), "Wrong number of rows"); 
            s_ -> add_column(col -> get_type());

            if(size_ == cap_) grow_();
            cols_[size_] = col;
            ++size_;
        }

        /** Return the value at the given column and row. Accessing rows or
        *  columns out of bounds, or request the wrong type is undefined.*/
        // indices should be in bounds
        // col_type should be int
        int get_int(size_t col, size_t row) {
            check(get_col_(col) -> get_type() == 'I', "Type not int");
            // these two methods check the indices
            return get_col_(col) -> as_int() -> get(row);
        }

        bool get_bool(size_t col, size_t row) {
            check(get_col_(col) -> get_type() == 'B', "Type not bool");
            // these two methods check the indices
            return get_col_(col) -> as_bool() -> get(row);
        }

        float get_float(size_t col, size_t row) {
            check(get_col_(col) -> get_type() == 'F', "Type not float");
            // these two methods check the indices
            return get_col_(col) -> as_float() -> get(row);
        }

        String* get_string(size_t col, size_t row) {
            check(get_col_(col) -> get_type() == 'S', "Type not string");
            // these two methods check the indices
            return get_col_(col) -> as_string() -> get(row);
        }

        /** Set the value at the given column and row to the given value.
        * If the column is not  of the right type or the indices are out of
        * bound, the result is undefined. */
        void set(size_t col, size_t row, int val) {
            check(get_col_(col) -> get_type() == 'I', "Wrong type");
            get_col_(col) -> as_int() -> set(row, val);
        }

        void set(size_t col, size_t row, bool val) {
            check(get_col_(col) -> get_type() == 'B', "Wrong type");
            get_col_(col) -> as_bool() -> set(row, val);
        }
        
        void set(size_t col, size_t row, float val) {
            check(get_col_(col) -> get_type() == 'F', "Wrong type");
            get_col_(col) -> as_float() -> set(row, val);
        }
        
        void set(size_t col, size_t row, String* val) {
            check(get_col_(col) -> get_type() == 'S', "Wrong type");
            get_col_(col) -> as_string() -> set(row, val);
        }

        // creates a dataframe with a single column that contains the given array of data
        static DataFrame* from_array(size_t size, bool* vals) {
            Schema* s = new Schema(0, size);
            s->add_column('B');
            DataFrame* out = new DataFrame(*s);
            for (size_t i = 0; i < size; ++i) out->set(0, i, vals[i]);
            delete s;
            return out;
        }

        static DataFrame* from_array(size_t size, int* vals) {
            Schema* s = new Schema(0, size);
            s->add_column('I');
            DataFrame* out = new DataFrame(*s);
            for (size_t i = 0; i < size; ++i) out->set(0, i, vals[i]);
            delete s;
            return out;
        }

        static DataFrame* from_array(size_t size, float* vals) {
            Schema* s = new Schema(0, size);
            s->add_column('F');
            DataFrame* out = new DataFrame(*s);
            for (size_t i = 0; i < size; ++i) out->set(0, i, vals[i]);
            delete s;
            return out;
        }

        static DataFrame* from_array(size_t size, String** vals) {
            Schema* s = new Schema(0, size);
            s->add_column('S');
            DataFrame* out = new DataFrame(*s);
            for (size_t i = 0; i < size; ++i) out->set(0, i, vals[i]);
            delete s;
            return out;
        }

        // creates a dataframe with a single column and a single row from the given scalar
        static DataFrame* from_scalar(bool val) { return from_array(1, &val); }

        static DataFrame* from_scalar(int val) { return from_array(1, &val); }

        static DataFrame* from_scalar(float val) { return from_array(1, &val); }

        static DataFrame* from_scalar(String* val) { return from_array(1, &val); }

        /** Set the fields of the given row object with values from the columns at
        * the given offset.  If the row is not form the same schema as the
        * dataframe, results are undefined.
        */
        void fill_row(size_t idx, Row& row) {
            check(row.width() == s_->width(), "Mismatched widths");
            row.set_idx(idx);
            for(size_t i = 0; i < row.width(); ++i) {
                // don't need to check that types match because as_[type] fails if wrong
                if(row.col_type(i) == 'B') {
                    row.set(i, get_col_(i) -> as_bool() -> get(idx));
                } else if (row.col_type(i) == 'I') {
                    row.set(i, get_col_(i) -> as_int() -> get(idx));
                } else if (row.col_type(i) == 'F') {
                    row.set(i, get_col_(i) -> as_float() -> get(idx));
                } else if (row.col_type(i) == 'S') {
                    row.set(i, get_col_(i) -> as_string() ->get(idx));
                } else check(false, "Invalid type");
            } 
        
        }

        /** Add a row at the end of this dataframe. The row is expected to have
        *  the right schema and be filled with values, otherwise undedined.  */
        void add_row(Row& row) {
            check(row.width() == s_ -> width(), "Mismatched widths");
            for(size_t i = 0; i< row.width(); ++i) {
                // don't need to check that types match because as_[type] fails if wrong
                if (row.col_type(i) == 'B') 
                    get_col_(i)->as_bool()->push_back(row.get_bool(i));
                else if (row.col_type(i) == 'I') 
                    get_col_(i)->as_int()->push_back(row.get_int(i));
                else if (row.col_type(i) == 'F') 
                    get_col_(i)->as_float()->push_back(row.get_float(i));
                else if (row.col_type(i) == 'S') 
                    get_col_(i)->as_string()->push_back(row.get_string(i));
                else check(false, "Invalid type");
            }
            s_->add_row();
        }

        /** The number of rows in the dataframe. */
        size_t nrows() { return s_->length(); }

        /** The number of columns in the dataframe.*/
        size_t ncols() { return s_->width(); }

        // updates the stored row to match this dataframe's current
        // deletes the old row since its internal
        // this is a private method
        void update_row_() {
            delete row_;
            row_ = new Row(*s_);
        }

        /** Visit rows in order */
        void map(Rower& r) {
            update_row_();
            for (size_t i = 0; i < nrows(); ++i) {
                fill_row(i, *row_);
                r.accept(*row_);
            }
        }

        // maps over the given range of rows
        // inclusive start, non-inclusive end
        static void map_over_range_(PMapData* pmd) {
            DataFrame* df = pmd->df_;
            Rower* r = pmd->r_;
            size_t start = pmd->start_;
            size_t end = pmd->end_;
            Row* row = new Row(df->get_schema());
            for (size_t i = start; i < end; ++i) {
                df->fill_row(i, *row);
                r->accept(*row);
            }
            delete row;
            delete pmd;
        }

        void pmap(Rower& r) {
            const int nthreads = 16;
            std::thread* pool[nthreads];
            Rower* rowers[nthreads];
            size_t dr = nrows() / nthreads;
            for (size_t i = 0; i < nthreads; ++i) {
                // for each thread
                size_t start = i * dr;
                size_t end;
                if (i == nthreads - 1) end = nrows();
                else end = (i+1) * dr;

                Rower* new_rower = dynamic_cast<Rower*>(r.clone());
                // thread is responsible for freeing this data
                PMapData* pmd = new PMapData(this, new_rower, start, end);
                pool[i] = new std::thread(map_over_range_, pmd);
                check(pool[i] != nullptr, "Null thread");
                rowers[i] = new_rower;
            }

            for (size_t i = 0; i < nthreads; ++i) {
                pool[i]->join();
                r.join_delete(rowers[i]);
            }
        }

        /** Create a new dataframe, constructed from rows for which the given Rower
        * returned true from its accept method. */
        DataFrame* filter(Rower& r) {
            update_row_();
            // creates a dataframe with no rows
            DataFrame* out = new DataFrame(*this);
            for (size_t i = 0; i < nrows(); ++i) {
                fill_row(i, *row_);
                if (r.accept(*row_)) {
                    out->add_row(*row_);
                }
            }
            return out;
        }

        // serializes this DataFrame into the following format:
        // <col_types> <nrows> [[<data00> <data01> <data02> ...] [...] ...]
        // column: [<data0> <data1> <data2>]
        char* serialize() {
            StrBuff* sb = new StrBuff();
            char* cts = new char[s_->width() + 1]; // adds 1 extra spot for null term
            memcpy(cts, s_->col_types_, (s_->width()) * sizeof(char));
            cts[s_->width()] = '\0';
            sb->c(cts);
            delete[] cts;
            sb->c(DLM);
            sb->c(nrows());
            sb->c(DLM);
            sb->c('[');

            char* tmp;            
            for (size_t i = 0; i < ncols(); ++i) {
                if (i != 0) sb->c(DLM);
                tmp = cols_[i]->serialize();
                sb->c(tmp);
                delete[] tmp;
            }

            sb->c(']');
            char* out = sb->no_cpy_get();
            delete sb;
            return out;
        }

        // deserializes the given string into a DataFrame
        static DataFrame* deserialize(char* m) {
            char* rest = nullptr;
            char* tok;
            char* col_types = next_token(m, &rest, DLM, false);
            size_t ncols = strlen(col_types);

            tok = next_token(rest, &rest, DLM, false);
            size_t nrows = atoi(tok);
            delete[] tok;
            // skip to beginning of columns
            delete[] next_token(rest, &rest, '[', false);

            Schema* s = new Schema(0, nrows);
            DataFrame* df = new DataFrame(*s);

            for (size_t i = 0; i < ncols; ++i) {
                if (i != 0) delete[] next_token(rest, &rest, ' ', false); // consume next space
                tok = next_token(rest, &rest, ']', false); // column handles removing escapes
                Column* c = Column::deserialize(tok, col_types[i], nrows);
                delete[] tok;
                df->add_column(c);
            }

            delete[] col_types;
            delete s;
            return df;
        }

        /** Print the dataframe in SoR format to standard output. */
        void print() {
            update_row_();
            RowPrinter* rp = new RowPrinter();
            // note: consider using map?
            for (size_t i = 0; i < s_->length(); ++i) {
                fill_row(i, *row_);
                rp->accept(*row_);
                if (i != s_->length() - 1) printf("\n");
            }
            delete rp;
        }
};
