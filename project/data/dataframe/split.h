// Authors: Zoe Corning (corning.z@husky.neu.edu) & Rucha Khanolkar (khanolkar.h@husky.neu.edu)
# pragma once

#include "dataframe.h"
#include "schema.h"
#include "column.h"
#include "rower.h"

// accepts a row and puts it into the correct DataFrame (chunk)
// Order of rows is not preserved
class Splitter : public Rower {
    public:
        DataFrame** dfs_; // dataframes containing split up data
        size_t n_; // number of resulting dataframes
        size_t next_; // index of next DataFrame to receive a row
        
        // creates a splitter with n empty dataframes
        Splitter(size_t n, const char* col_types) {
            check(n > 0, "Cannot split into less than 2 DataFrames");
            n_ = n;
            dfs_ = new DataFrame*[n];
            Schema* s = new Schema(col_types); // ncols = cols, 0 = rows
            for (size_t i = 0; i < n; ++i) {
                dfs_[i] = new DataFrame(*s);
            }
            delete s;
            next_ = 0;
        }

        // adds the given row to one of the smaller dataframes
        bool accept(Row& r) {
            check(dfs_ != nullptr, "This splitter cannot accept more rows");
            dfs_[next_]->add_row(r);
            next_ = mod(next_ + 1, n_);
            return true;
        }

        // gets the resulting split up data
        // Note: can only be called once, and splitter cannot accept more rows after this call
        DataFrame** get_dfs() { 
            DataFrame** out = dfs_;
            dfs_ = nullptr;
            return out;
        }

        // no join_delete(), too annoying and slow to merge dataframes afterwards
        // won't create an even enough split
};

// splits the given dataframe into n dataframes by row
// returns array of DataFrame* of size n with nrows = df->nrows()/n
DataFrame** split_by_row(DataFrame* df, size_t n) {
    Splitter* sp = new Splitter(n, df->get_schema().col_types_);
    df->map(*sp);
    DataFrame** out = sp->get_dfs();
    delete sp;
    return out;
}
