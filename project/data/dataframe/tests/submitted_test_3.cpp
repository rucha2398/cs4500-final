// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include <gtest/gtest.h>
#include "../row.h"
#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../../../util/helper.h"
#include "../schema.h"

// This test file includes all the tests for the Row class

#define CS4500_ASSERT_TRUE(a)  \
    ASSERT_EQ((a),true);
#define CS4500_ASSERT_FALSE(a) \
    ASSERT_EQ((a),false);
#define CS4500_ASSERT_EXIT_ZERO(a)  \
    ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

// copy constructor
void test1() {

    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1->add_row();
    s1->add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);

    CS4500_ASSERT_TRUE(r1->width() == 5);

    //delete everything
    delete s1;
    delete r1;
    
    exit(0);
}
TEST(W1, test1) {
    CS4500_ASSERT_EXIT_ZERO(test1)
}

// set for bool, int, float and string
void test2() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1->add_row();
    s1->add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);
    Row* r2 = new Row(*s1);

    CS4500_ASSERT_TRUE(r1 -> width() == 5);

    //setting for bool
    bool b1 = 0;
    bool b2 = 1;
    r1 -> set(0, b1);
    r2 -> set(0, b2);

    //setting for int
    r1 -> set(1, 6);
    r2 -> set(1, -9);

    //setting for float
    float f1 = 42.9;
    float f2 = -89.9;
    r1 -> set(2, f1);
    r2 -> set(2, f2);

    //setting for string
    String* string_1 = new String("hello");
    String* string_2 = new String("world");
    r1 -> set(3, string_1);
    r2 -> set(3, string_2);

    //setting for string
    String* string_3 = new String("test");
    String* string_4 = new String("dogs");
    r1 -> set(4, string_3);
    r2 -> set(4, string_4);

    CS4500_ASSERT_TRUE(r1 -> width() == 5);
    CS4500_ASSERT_TRUE(r2 -> width() == 5);

    //delete everything
    delete s1;
    delete r1;
    delete r2;
    delete string_1;
    delete string_2;
    delete string_3;
    delete string_4;
    
    exit(0);
}

TEST(W1, test2) {
    CS4500_ASSERT_EXIT_ZERO(test2)
}

// testing setters and getters (bool, int, float, string)
void test3() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1->add_row();
    s1->add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);
    Row* r2 = new Row(*s1);

    CS4500_ASSERT_TRUE(r1 -> width() == 5);
    CS4500_ASSERT_TRUE(r2 -> width() == 5);

    //setting for bool
    bool b1 = 0;
    bool b2 = 1;
    r1 -> set(0, b1);
    r2 -> set(0, b2);

    //getting for bool
    CS4500_ASSERT_TRUE(r1 -> get_bool(0) == 0);
    CS4500_ASSERT_TRUE(r2 -> get_bool(0) == 1);

    //setting for int
    r1 -> set(1, 6);
    r2 -> set(1, -9);

    //getting for int
    CS4500_ASSERT_TRUE(r1 -> get_int(1) == 6);
    CS4500_ASSERT_TRUE(r2 -> get_int(1) == -9);

    //setting for float
    float f1 = 42.9;
    float f2 = -89.9;
    r1 -> set(2, f1);
    r2 -> set(2, f2);


    //getting for float
    CS4500_ASSERT_TRUE(float_eq((r1 -> get_float(2)),6));
    CS4500_ASSERT_TRUE(float_eq((r2 -> get_float(2)),-9));

    //setting for string
    String* string_1 = new String("hello");
    String* string_2 = new String("world");
    r1 -> set(3, string_1);
    r2 -> set(3, string_2);

    //getting for string
    CS4500_ASSERT_TRUE(string_1 -> equals(r1 -> get_string(3)));
    CS4500_ASSERT_TRUE(string_2 -> equals(r2 -> get_string(3)));

    //setting for string
    String* string_3 = new String("test");
    String* string_4 = new String("dogs");
    r1 -> set(4, string_3);
    r2 -> set(4, string_4);

    //getting for string
    CS4500_ASSERT_TRUE(string_3 -> equals(r1 -> get_string(4)));
    CS4500_ASSERT_TRUE(string_4 -> equals(r2 -> get_string(4)));

    //delete everything
    delete s1;
    delete r1;
    delete r2;
    delete string_1;
    delete string_2;
    delete string_3;
    delete string_4;
    
    exit(0);
    
}

TEST(W1, test3) {
    CS4500_ASSERT_EXIT_ZERO(test3)
}

// set_idx and get_idx
void test4() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1->add_row();
    s1->add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);
    Row* r2 = new Row(*s1);

    CS4500_ASSERT_TRUE(r1 -> width() == 5);
    CS4500_ASSERT_TRUE(r2 -> width() == 5);

    //setting index
    r1 -> set_idx(0);
    r2 -> set_idx(1);

    //getting index
    CS4500_ASSERT_TRUE(r1 -> get_idx() == 0);
    CS4500_ASSERT_TRUE(r2 -> get_idx() == 1);

    delete s1;
    delete r1;
    delete r2;

    exit(0);
}
TEST(W1, test4) {
    CS4500_ASSERT_EXIT_ZERO(test4)
}

// col_type
void test5() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1->add_row();
    s1->add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);
    Row* r2 = new Row(*s1);

    CS4500_ASSERT_TRUE(r1 -> width() == 5);
    CS4500_ASSERT_TRUE(r2 -> width() == 5);

    //col_type testing
    CS4500_ASSERT_TRUE(r1 -> col_type(0) == 'B');
    CS4500_ASSERT_TRUE(r1 -> col_type(1) == 'I');
    CS4500_ASSERT_TRUE(r1 -> col_type(2) == 'F');
    CS4500_ASSERT_TRUE(r1 -> col_type(3) == 'S');
    CS4500_ASSERT_TRUE(r1 -> col_type(4) == 'S');
    
    delete s1;
    delete r1;
    delete r2;

    exit(0);
}
TEST(W1, test5) {
    CS4500_ASSERT_EXIT_ZERO(test5)
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
