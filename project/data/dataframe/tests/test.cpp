#include <assert.h>
#include <time.h>
#include <thread>
#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../schema.h"
#include "../row.h"
#include "../fielder.h"
#include "../column.h"
#include "../dataframe.h"

bool float_eq(float f1, float f2) {
    return (f1 - f2) < 0.01 || (f1 - f2) > -0.01;
}

class MatchElement : public Fielder {
    public:
        DataFrame* df_;
        size_t row;
        size_t idx;

        // takes a dataframe to match
        MatchElement(DataFrame* df) {
            df_ = df;
            idx = -1;
        }

        void start(size_t r) {
            row = r;
            idx = 0;
        }

        void accept(bool b) {
            assert(df_->get_schema().col_type(idx) == 'B');
            assert(df_->get_bool(idx, row) == b);
            ++idx;
        }

        void accept(int i) {
            assert(df_->get_schema().col_type(idx) == 'I');
            assert(df_->get_int(idx, row) == i);
            ++idx;
        }

        void accept(float f) {
            assert(df_->get_schema().col_type(idx) == 'F');
            assert(float_eq(df_->get_float(idx, row), f));
            ++idx;
        }

        void accept(String* s) {
            assert(df_->get_schema().col_type(idx) == 'S');
            if (s == nullptr) assert(df_->get_string(idx, row) == nullptr);
            else assert(df_->get_string(idx, row)->equals(s));
            ++idx;
        }

        void done() {
            assert(df_->ncols() == idx);
        }
};

class MatchRow : public Rower {
    public:
        DataFrame* df_;
        MatchElement* me_;

        // takes a dataframe to match
        MatchRow(DataFrame* df) {
            df_ = df;
            me_ = new MatchElement(df);
        }

        ~MatchRow() {
            delete me_;
        }

        bool accept(Row& r) {
            r.visit(r.get_idx(), *me_);
        }

        Object* clone() {
            return new MatchRow(df_);
        }
};

class ExpensiveFielder: public Fielder {
    public:
        // takes a dataframe to match

        void start(size_t r) {}

        void accept(bool b) {
            for (int i = 0; i < 1000; ++i) {
                b = !b;
            }
        }

        void accept(int n) {
            for (int i = 0; i < 1000; ++i) {
                ++n;
            }
        }
        
        void accept(float f) {
            for (int i = 0; i < 1000; ++i) {
                ++f;
            } 
        }

        void accept(String* s) {
            int count = 0;
            for (int i = 0; i < 1000; ++i) {
                ++count;
            } 
        }

        void done() {}
};

class ExpensiveRower : public Rower {
    public:
        ExpensiveFielder* ef_;
        
        ExpensiveRower() {
            ef_ = new ExpensiveFielder();
        }

        bool accept(Row& r) {
            r.visit(r.get_idx(), *ef_);
        }

        Object* clone() {
            return new ExpensiveRower();
        }
};

//map
void test_map(DataFrame* df, Rower* r) {
    df->map(*r);
}

//pmap
void test_pmap(DataFrame* df, Rower* r) {
    df->pmap(*r);
}

DataFrame* generateData() {
    int ncols = 16;
    int colLength = ((100*1024*1024)/4)/ncols;
    // build up schema
    Schema* s = new Schema();
    // adding columns
    for (int i = 0; i < ncols; ++i) {
        if (i % 4 == 0) {
            s->add_column('B', nullptr);
        } else if (i % 4 == 1) {
            s->add_column('I', nullptr);
        } else if (i % 4 == 2) {
            s->add_column('F', nullptr);
        } else if (i % 4 == 3) {
            s->add_column('S', nullptr);
        }
    }
    // adding rows
    for (int i = 0; i < colLength; ++i) {
        s->add_row(nullptr);
    }

    DataFrame* df = new DataFrame(*s);
    for (int i = 0; i < ncols; ++i) {
        for (int j = 0; j < colLength; ++j) { 
            if (i % 4 == 0) {
                df->set(i, j, i > 8);
            } else if (i % 4 == 1) {
                df->set(i, j, j);
            } else if (i % 4 == 2) {
                float f = j;
                df->set(i, j, f);
            } else if (i % 4 == 3) {
                String* str = new String("hello");
                df->set(i, j, str);
            }
        }
    }

    return df;
}



int main() {
    clock_t start, end;
    
    puts("generating data");
    DataFrame* df = generateData();
    //df->print()
    
    MatchRow* mr = new MatchRow(df);
    
    puts("testing map");
    start = clock();
    test_map(df, mr);
    end = clock();
    printf("took %f time\n", (double)(end-start)/CLOCKS_PER_SEC);
    
    puts("testing pmap");
    start = clock();
    test_pmap(df, mr);
    end = clock();
    printf("took %f time\n", (double)(end-start)/CLOCKS_PER_SEC/16);
    
    ExpensiveRower* er = new ExpensiveRower();
    
    puts("testing map expensive");
    start = clock();
    test_map(df, er);
    end = clock();
    printf("took %f time\n", (double)(end-start)/CLOCKS_PER_SEC);
    
    puts("testing pmap expensive");
    start = clock();
    test_pmap(df, er);
    end = clock();
    printf("took %f time\n", (double)(end-start)/CLOCKS_PER_SEC/16);
    
    delete df;
    puts("done");
    return 0;
}
