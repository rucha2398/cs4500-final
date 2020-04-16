// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include <gtest/gtest.h>
#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../schema.h"

// This file contains all the tests for the Schema class

#define CS4500_ASSERT_TRUE(a)  \
    ASSERT_EQ((a),true);
#define CS4500_ASSERT_FALSE(a) \
    ASSERT_EQ((a),false);
#define CS4500_ASSERT_EXIT_ZERO(a)  \
    ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

// empty constructor
void test1() {
    Schema* s = new Schema();
    CS4500_ASSERT_TRUE(s->width() == 0);
    CS4500_ASSERT_TRUE(s->length() == 0);
    exit(0);
}
TEST(W1, test1) {
    CS4500_ASSERT_EXIT_ZERO(test1)
}

// Copy Constructor
void test2() {
    Schema* s1 = new Schema();
    
    s1->add_column('S');
    s1->add_row();
    s1->add_column('B');
    s1->add_column('I');
    s1->add_column('F');
    
    Schema* s2 = new Schema(*s1);
    
    CS4500_ASSERT_TRUE(s2->col_type(0) == 'S');
    CS4500_ASSERT_TRUE(s2->col_type(1) == 'B');
    CS4500_ASSERT_TRUE(s2->col_type(2) == 'I');
    CS4500_ASSERT_TRUE(s2->col_type(3) == 'F');

    CS4500_ASSERT_TRUE(s2->width() == 4);
    CS4500_ASSERT_TRUE(s2->length() == 1);

    delete s1;
    delete s2;

    exit(0);
}
TEST(W1, test2) {
    CS4500_ASSERT_EXIT_ZERO(test2)
}

// types constructor
void test3() {
    Schema* s1 = new Schema("IFSBSIFF");
    Schema* s2 = new Schema("");

    CS4500_ASSERT_TRUE(s1->width() == 8);
    CS4500_ASSERT_TRUE(s1->length() == 0);
    CS4500_ASSERT_TRUE(s2->width() == 0);
    CS4500_ASSERT_TRUE(s2->length() == 0);

    CS4500_ASSERT_TRUE(s1->col_type(0) == 'I');
    CS4500_ASSERT_TRUE(s1->col_type(1) == 'F');
    CS4500_ASSERT_TRUE(s1->col_type(2) == 'S');
    CS4500_ASSERT_TRUE(s1->col_type(3) == 'B');
    CS4500_ASSERT_TRUE(s1->col_type(7) == 'F');

    delete s1;
    delete s2;

    exit(0);
}
TEST(W1, test3) {
    CS4500_ASSERT_EXIT_ZERO(test3)
}

// add_column
void test4() {
    Schema* s1 = new Schema();

    s1 -> add_column('B');
    s1 -> add_column('I');
    s1 -> add_column('F');
    s1 -> add_column('S');

    CS4500_ASSERT_TRUE(s1 -> col_type(0) == 'B');
    CS4500_ASSERT_TRUE(s1 -> col_type(1) == 'I');
    CS4500_ASSERT_TRUE(s1 -> col_type(2) == 'F');
    CS4500_ASSERT_TRUE(s1 -> col_type(3) == 'S');

    CS4500_ASSERT_TRUE(s1 -> width() == 4);

    s1 -> add_column('S');
    CS4500_ASSERT_TRUE(s1 -> width() == 5);
    CS4500_ASSERT_TRUE(s1 -> col_type(4) == 'S');

    delete s1;

    exit(0);
}
TEST(W1, test4) {
    CS4500_ASSERT_EXIT_ZERO(test4)
}

// add_row
void test5() {
    Schema* s1 = new Schema();

    s1 -> add_row();
    s1 -> add_row();

    CS4500_ASSERT_TRUE(s1 -> length() == 2);

    delete s1;

    exit(0);
}
TEST(W1, test5) {
    CS4500_ASSERT_EXIT_ZERO(test5)
}

// col_name
// col_type
void test6() {
    Schema* s1 = new Schema();

    s1 -> add_column('B');
    s1 -> add_column('I');
    s1 -> add_column('F');
    s1 -> add_column('S');
    s1 -> add_column('S');
    s1 -> add_row();
    s1 -> add_row();

    CS4500_ASSERT_TRUE(s1 -> col_type(0) == 'B');
    CS4500_ASSERT_TRUE(s1 -> col_type(1) == 'I');
    CS4500_ASSERT_TRUE(s1 -> col_type(2) == 'F');
    CS4500_ASSERT_TRUE(s1 -> col_type(3) == 'S');
    CS4500_ASSERT_TRUE(s1 -> col_type(3) == 'S');

    delete s1;

    exit(0);
}
TEST(W1, test6) {
    CS4500_ASSERT_EXIT_ZERO(test6)
}

// width()
// length()
void test7() {
    Schema* s1 = new Schema();
    CS4500_ASSERT_TRUE(s1 -> width() == 0);
    CS4500_ASSERT_TRUE(s1 -> length() == 0);

    s1 -> add_column('B');
    CS4500_ASSERT_TRUE(s1 -> width() == 1);
    CS4500_ASSERT_TRUE(s1 -> length() == 0);

    s1 -> add_row();
    CS4500_ASSERT_TRUE(s1 -> width() == 1);
    CS4500_ASSERT_TRUE(s1 -> length() == 1);

    s1 -> add_column('I');
    CS4500_ASSERT_TRUE(s1 -> width() == 2);
    CS4500_ASSERT_TRUE(s1 -> length() == 1);

    s1 -> add_column('F');
    CS4500_ASSERT_TRUE(s1 -> width() == 3);
    CS4500_ASSERT_TRUE(s1 -> length() == 1);

    s1 -> add_column('S');
    CS4500_ASSERT_TRUE(s1 -> width() == 4);
    CS4500_ASSERT_TRUE(s1 -> length() == 1);
    
    s1 -> add_row();
    CS4500_ASSERT_TRUE(s1 -> width() == 4);
    CS4500_ASSERT_TRUE(s1 -> length() == 2);

    s1 -> add_column('S');
    CS4500_ASSERT_TRUE(s1 -> width() == 5);

    delete s1;

    exit(0);
}
TEST(W1, test7) {
    CS4500_ASSERT_EXIT_ZERO(test7)
}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
