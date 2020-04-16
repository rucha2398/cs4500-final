// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include <gtest/gtest.h>
#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../../../util/helper.h"
#include "../row.h"
#include "../schema.h"
#include "../fielder.h"

#define CS4500_ASSERT_TRUE(a)  \
    ASSERT_EQ((a),true);
#define CS4500_ASSERT_FALSE(a) \
    ASSERT_EQ((a),false);
#define CS4500_ASSERT_EXIT_ZERO(a)  \
    ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

// This file tests the Fielder class and the Row class's visit method

// class that makes sure that the visited fields match the fields of the initialized row
// uused in test6 only
class MatchRow : public Fielder {
    public:
        Row* r; // external
        size_t idx; // index into row - number of times fielder has been called
        MatchRow(Row* row) {
            idx = -1;
            r = row;
        }

        void start(size_t r) { idx = 0; }

        void accept(bool b) {
            CS4500_ASSERT_TRUE(r->col_type(idx) == 'B');
            CS4500_ASSERT_TRUE(r->get_bool(idx) == b);
            ++idx;
        }
        void accept(int i) {
            CS4500_ASSERT_TRUE(r->col_type(idx) == 'I');
            CS4500_ASSERT_TRUE(r->get_int(idx) == i);
            ++idx;
        }
        void accept(float f) {
            CS4500_ASSERT_TRUE(r->col_type(idx) == 'F');
            CS4500_ASSERT_TRUE(float_eq(r->get_float(idx), f));
            ++idx;
        }
        void accept(String* s) {
            CS4500_ASSERT_TRUE(r->col_type(idx) == 'S');
            CS4500_ASSERT_TRUE(r->get_string(idx)->equals(s));
            ++idx;
        }

        virtual void done() {
            CS4500_ASSERT_TRUE(r->width() == idx);
        }
};

// visit using MatchRow fielder
void test1() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1->add_row();
    s1->add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);

    bool b = 1;
    r1->set(0, b);

    int i = -12;
    r1->set(1, i);

    float f = -12.12;
    r1->set(2, f);

    String* str1 = new String("don't look at");
    String* str2 = new String("these tests");
    r1->set(3, str1);
    r1->set(4, str2);

    MatchRow* mr = new MatchRow(r1);

    r1->set_idx(1);

    r1->visit(1, *mr);

    delete s1;
    delete r1;
    delete str1;
    delete str2;
    delete mr;

    exit(0);
}
TEST(W1, test1) {  
    CS4500_ASSERT_EXIT_ZERO(test1)
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
