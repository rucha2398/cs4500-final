// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include <gtest/gtest.h>
#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../../../util/helper.h"
#include "../schema.h"
#include "../row.h"
#include "../fielder.h"
#include "../column.h"
#include "../dataframe.h"

#define CS4500_ASSERT_TRUE(a)  \
    ASSERT_EQ((a),true);
#define CS4500_ASSERT_FALSE(a) \
    ASSERT_EQ((a),false);
#define CS4500_ASSERT_EXIT_ZERO(a)  \
    ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

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
            CS4500_ASSERT_TRUE(df_->get_schema().col_type(idx) == 'B');
            CS4500_ASSERT_TRUE(df_->get_bool(idx, row) == b);
            ++idx;
        }

        void accept(int i) {
            CS4500_ASSERT_TRUE(df_->get_schema().col_type(idx) == 'I');
            CS4500_ASSERT_TRUE(df_->get_int(idx, row) == i);
            ++idx;
        }

        void accept(float f) {
            CS4500_ASSERT_TRUE(df_->get_schema().col_type(idx) == 'F');
            CS4500_ASSERT_TRUE(float_eq(df_->get_float(idx, row), f));
            ++idx;
        }

        void accept(String* s) {
            assert(df_->get_schema().col_type(idx) == 'S');
            if (s == nullptr) assert(df_->get_string(idx, row) == nullptr);
            else assert(df_->get_string(idx, row)->equals(s));
            ++idx;
        }

        void done() {
            CS4500_ASSERT_TRUE(df_->ncols() == idx);
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

//map
void test9() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1 -> add_row();
    s1 -> add_row();


    //add schema to row constructor
    Row* r1 = new Row(*s1);
    Row* r2 = new Row(*s1);

    // fill up the rows with data
    bool b1 = 0;
    bool b2 = 1;
    int i1 = 12;
    int i2 = -1;
    float f1 = 12.12;
    float f2 = -13.4;
    String* str1 = new String("hello");
    String* str2 = new String("world");
    r1->set(0, b1);
    r1->set(1, i1);
    r1->set(2, f1);
    r1->set(3, str1);
    r1->set(4, str2);

    r2->set(0, b2);
    r2->set(1, i2);
    r2->set(2, f2);
    r2->set(3, new String("world"));
    r2->set(4, new String("hello"));

    //dataframe
    DataFrame* df = new DataFrame(*s1);

    //adds row to end of this dataframe
    df -> add_row(*r1);
    df -> add_row(*r2);

    CS4500_ASSERT_TRUE(df -> nrows() == 4);

    MatchRow* mr = new MatchRow(df);

    df->map(*mr);
    df->pmap(*mr);

    delete s1;
    delete r1;
    delete r2;
    delete df;
    delete mr;

    exit(0);
}

TEST(W1, test9) {
    CS4500_ASSERT_EXIT_ZERO(test9)
}

//filter
void test10() {
    //TODO
    exit(0);
}

TEST(W1, test10) {
    CS4500_ASSERT_EXIT_ZERO(test10)
}

//serialization and deserialization
void test11() {
    //create schema
    Schema* s = new Schema("BIS");
    // Note: don't test float column since it's tested in column
    s -> add_row();
    s -> add_row();
    s -> add_row();

    DataFrame* d = new DataFrame(*s);
    // col 0: 1 0 1
    bool b = 1;
    d->set(0, 0, b);
    b = 0;
    d->set(0, 1, b);
    b = 1;
    d->set(0, 2, b);

    // col 1: 12 -15 2
    d->set(1, 0, 12);
    d->set(1, 1, -15);
    d->set(1, 2, 2);

    // col 2: <s1> <s2> <s3>
    String* s1 = new String("]he} llo\n");
    String* s2 = new String("world \\");
    String* s3 = new String("\\");
    d->set(2, 0, s1);
    d->set(2, 1, s2);
    d->set(2, 2, s3);

    char* ds = d->serialize();
    //printf("%s\n", ds);
    check(streq(ds, "BIS 3 [[1 0 1] [12 -15 2] [\\]he\\}\\ llo\\\n world\\ \\\\ \\\\]]"), 
            "DataFrame serialization failed");

    DataFrame* dd = DataFrame::deserialize(ds);
    for (int i = 0; i < 3; ++i) check(dd->get_bool(0, i) == d->get_bool(0, i), "Mismatched bools");
    for (int i = 0; i < 3; ++i) check(dd->get_int(1, i) == d->get_int(1, i), "Mismatched ints");
    for (int i = 0; i < 3; ++i) check(dd->get_string(2, i)->equals(d->get_string(2, i)), "Mismatched strs");

    delete s;
    delete d;
    delete[] ds;
    delete dd;

    exit(0);
}

TEST(W1, test11) {
    CS4500_ASSERT_EXIT_ZERO(test11)
}

//print
void test12() {
    //TODO
    exit(0);
}

TEST(W1, test12) {
    CS4500_ASSERT_EXIT_ZERO(test12)
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
