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

// tests our DataFrame class 

//copy constructor
void test1() {
    Schema* s1 = new Schema();

    s1 -> add_column('B');
    s1 -> add_column('I');
    s1 -> add_column('F');
    s1 -> add_column('S');
    s1 -> add_column('S');
    s1 -> add_row();
    s1 -> add_row();

    //dataframe
    DataFrame* df = new DataFrame(*s1);

    //copy constructor 
    DataFrame* df2 = new DataFrame(*df);

    //testing the copy constructor
    CS4500_ASSERT_TRUE(df2->get_schema().col_type(2) == df->get_schema().col_type(2));
    CS4500_ASSERT_TRUE(df2->nrows() == 0);
    CS4500_ASSERT_TRUE(df2->ncols() == df->ncols());
    CS4500_ASSERT_TRUE(df2->get_schema().col_type(1) == df->get_schema().col_type(1));

    delete s1;
    delete df;
    delete df2;

    exit(0);
}
TEST(W1, test1) {
    CS4500_ASSERT_EXIT_ZERO(test1)
}

//constructor with schema
void test2() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1 -> add_row();
    s1 -> add_row();

    DataFrame* df = new DataFrame(*s1);
    
    //testing schema constructor
    CS4500_ASSERT_TRUE(df->ncols() == s1->width());
    CS4500_ASSERT_TRUE(df->nrows() == s1->length());

    delete s1;
    delete df;
    
    exit(0);
}

TEST(W1, test2) {
    CS4500_ASSERT_EXIT_ZERO(test2)
}

//get_schema
void test3() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1 -> add_row();
    s1 -> add_row();

    DataFrame* df = new DataFrame(*s1);

    //returns the schema of the dataframe
    CS4500_ASSERT_TRUE(df->get_schema().width() == s1->width());
    CS4500_ASSERT_TRUE(df->get_schema().length() == s1->length());
    // TODO add more checks
   
    delete s1;
    delete df;

    exit(0);
}

TEST(W1, test3) {
    CS4500_ASSERT_EXIT_ZERO(test3)
}

//add column
void test4() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1 -> add_row();
    s1 -> add_row();

    DataFrame* df = new DataFrame(*s1);

    //create columns
    BoolColumn* bc = new BoolColumn();
    bc->push_back(1);
    bc->push_back(0);

    //adds a column
    df -> add_column(bc);

    //checks if schema added another column
    CS4500_ASSERT_TRUE(df->get_schema().col_type(5) == 'B');
    //checks the width of the schema
    CS4500_ASSERT_TRUE(df->get_schema().width() == 6);
  
    delete s1;
    delete df;

    exit(0);
}

TEST(W1, test4) {
    CS4500_ASSERT_EXIT_ZERO(test4)
}

//get(int, bool, float, string)
//set(int, bool, float, string)
void test5() {
    //create schema
    Schema* s1 = new Schema("BIFS");
    s1 -> add_row();
    s1 -> add_row();

    DataFrame* df = new DataFrame(*s1);

    //setting a bool
    bool b1 = 0;
    df -> set(0, 0, b1);
    //getting a bool
    CS4500_ASSERT_TRUE(df -> get_bool(0,0) == 0);

    //setting an int
    df -> set(1, 0, 6);
    //getting an int
    CS4500_ASSERT_TRUE(df -> get_int(1,0) == 6);

    //setting a float
    float f1 = 7.9;
    df -> set(2, 0, f1);
    //getting a float
    CS4500_ASSERT_TRUE(float_eq(df -> get_float(2,0), 7.9));

    //setting a string
    String* str1 = new String("hello");
    df -> set(3, 0, str1);
    //getting a string
    CS4500_ASSERT_TRUE(str1->equals(df -> get_string(3,0)));

    delete s1;
    delete df;

    exit(0);
}

TEST(W1, test5) {
    CS4500_ASSERT_EXIT_ZERO(test5)
}

//fill_row
void test6() {
    //create schema
    Schema* s1 = new Schema("BIFSS");
    s1 -> add_row();
    s1 -> add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);
    Row* r2 = new Row(*s1);

    //dataframe
    DataFrame* df = new DataFrame(*s1);
    // fill bool column
    bool b1 = 1;
    bool b2 = 0;
    df->set(0, 0, b1);
    df->set(0, 1, b2);
    // fill int column
    int i1 = 10;
    int i2 = -12;
    df->set(1, 0, i1);
    df->set(1, 1, i2);
    // fill float column
    float f1 = 1.21;
    float f2 = -12.12;
    df->set(2, 0, f1);
    df->set(2, 1, f2);
    // fill string columns
    String* str1 = new String("don't look here");
    String* str2 = new String("hello world");
    df->set(3, 0, str1);
    df->set(3, 1, str2);
    df->set(4, 0, new String("hello world"));
    df->set(4, 1, new String("don't look here"));

    //filling the row
    df -> fill_row(0, *r1);
    df -> fill_row(1, *r2);

    CS4500_ASSERT_TRUE(r1->get_bool(0) == b1);
    CS4500_ASSERT_TRUE(r2->get_bool(0) == b2);
    CS4500_ASSERT_TRUE(r1->get_int(1) == i1);
    CS4500_ASSERT_TRUE(r2->get_int(1) == i2);
    CS4500_ASSERT_TRUE(float_eq(r1->get_float(2), f1));
    CS4500_ASSERT_TRUE(float_eq(r2->get_float(2), f2));
    CS4500_ASSERT_TRUE(r1->get_string(3)->equals(str1));
    CS4500_ASSERT_TRUE(r2->get_string(3)->equals(str2));
    CS4500_ASSERT_TRUE(r1->get_string(4)->equals(str2));
    CS4500_ASSERT_TRUE(r2->get_string(4)->equals(str1));

    delete s1;
    delete r1;
    delete r2;
    delete df;

    exit(0);
}

TEST(W1, test6) {
    CS4500_ASSERT_EXIT_ZERO(test6)
}

//add_row
void test7() {
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

    //df->print();

    delete s1;
    delete r1;
    delete r2;
    delete df;

    exit(0);
}

TEST(W1, test7) {
    CS4500_ASSERT_EXIT_ZERO(test7)
}

//nrows and ncols
void test8() {
     //create schema
    Schema* s1 = new Schema("BIFS");
    s1 -> add_row();
    s1 -> add_row();

    //add schema to row constructor
    Row* r1 = new Row(*s1);
    Row* r2 = new Row(*s1);

    //dataframe
    DataFrame* df = new DataFrame(*s1);

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
    
    r2->set(0, b2);
    r2->set(1, i2);
    r2->set(2, f2);
    r2->set(3, str2);
   
    //adds row to end of this dataframe
    df -> add_row(*r1);
    df -> add_row(*r2);

    CS4500_ASSERT_TRUE(df -> nrows() == 4);
    CS4500_ASSERT_TRUE(df -> ncols() == 4);

    delete s1;
    delete r1;
    delete r2;
    delete df;

    exit(0);
}

TEST(W1, test8) {
    CS4500_ASSERT_EXIT_ZERO(test8)
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
