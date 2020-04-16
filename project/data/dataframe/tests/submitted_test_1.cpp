// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include <gtest/gtest.h>
#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../../../util/helper.h"
#include "../column.h"
#include <stdio.h>

// This file handles all of the testing for the column.h file and classes

#define CS4500_ASSERT_TRUE(a)  \
    ASSERT_EQ((a),true);
#define CS4500_ASSERT_FALSE(a) \
    ASSERT_EQ((a),false);
#define CS4500_ASSERT_EXIT_ZERO(a)  \
    ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

//testing column creation w/default constructor
void test1() {
    BoolColumn* bc = new BoolColumn();
    IntColumn* ic = new IntColumn();
    FloatColumn* fc = new FloatColumn();
    StringColumn* sc = new StringColumn();
    exit(0);
}

TEST(W1, test1) {
    CS4500_ASSERT_EXIT_ZERO(test1)
}

//testing ... constructor and get method
void test2() {
    BoolColumn* bc = new BoolColumn(4, 0, 1, 0, 0);
    IntColumn* ic = new IntColumn(4, 6, 8, 9, -4);
    FloatColumn* fc = new FloatColumn(0);
    FloatColumn* fc2 = new FloatColumn(2, 1, -8.3);

    String* s1 = new String("hello");
    String* s2 = new String("world");
    StringColumn* sc = new StringColumn(2, s1, s2);

    CS4500_ASSERT_TRUE(bc->get(0) == 0);
    CS4500_ASSERT_TRUE(bc->get(1) == 1);
    CS4500_ASSERT_TRUE(bc->get(2) == 0);
    CS4500_ASSERT_TRUE(bc->get(3) == 0);

    CS4500_ASSERT_TRUE(ic->get(0) == 6);
    CS4500_ASSERT_TRUE(ic->get(1) == 8);
    CS4500_ASSERT_TRUE(ic->get(2) == 9);
    CS4500_ASSERT_TRUE(ic->get(3) == -4);

    CS4500_ASSERT_TRUE(sc->get(0)->equals(s1));
    CS4500_ASSERT_TRUE(sc->get(1)->equals(s2));

    CS4500_ASSERT_TRUE(float_eq(fc2->get(0), 1));
    CS4500_ASSERT_TRUE(float_eq(fc2->get(1), -8.3));
    
    delete bc;
    delete ic;
    delete fc;
    delete fc2;
    delete sc;
    
    exit(0);
}

TEST(W1, test2) {
    CS4500_ASSERT_EXIT_ZERO(test2)
}

//test set method
void test3() {
    BoolColumn* bc = new BoolColumn(3, 0, 1, 0);
    IntColumn* ic = new IntColumn(2, 666, -400);
    FloatColumn* fc = new FloatColumn(2, 1, -8.3);

    String* s1 = new String("one");
    String* s2 = new String("two");
    String* s3 = new String("three");
    StringColumn* sc = new StringColumn(3, s3, s2, s1);
    
    bc->set(0, 0);
    bc->set(2, 1);
    CS4500_ASSERT_TRUE(bc->get(0) == 0);
    CS4500_ASSERT_TRUE(bc->get(1) == 1);
    CS4500_ASSERT_TRUE(bc->get(2) == 1);

    ic->set(0, -400);
    ic->set(1, -399);
    CS4500_ASSERT_TRUE(ic->get(0) == -400);
    CS4500_ASSERT_TRUE(ic->get(1) == -399);

    fc->set(0, -859);
    fc->set(1, 12.12);
    CS4500_ASSERT_TRUE(float_eq(fc->get(0), -859));
    CS4500_ASSERT_TRUE(float_eq(fc->get(1), 12.12));

    sc->set(2, s2);
    sc->set(1, s1);
    CS4500_ASSERT_TRUE(sc->get(0)->equals(s3));
    CS4500_ASSERT_TRUE(sc->get(1)->equals(s1));
    CS4500_ASSERT_TRUE(sc->get(2)->equals(s2));
    
    delete bc;
    delete ic;
    delete fc;
    delete sc;
    
    exit(0);
}

TEST(W1, test3) {
    CS4500_ASSERT_EXIT_ZERO(test3)
}

//testing as_TYPE(bool, int, float, string)
void test4() {
    BoolColumn* bc = new BoolColumn(4, 0, 1, 0, 0);
    CS4500_ASSERT_TRUE(bc -> as_bool() == bc);

    IntColumn* ic = new IntColumn(4, 6, 8, 9, -4);
    CS4500_ASSERT_TRUE(ic -> as_int() == ic);

    FloatColumn* fc = new FloatColumn(2, 1.1, -8.3);
    CS4500_ASSERT_TRUE(fc -> as_float() == fc);

    String* s1 = new String("hello");
    String* s2 = new String("world");
    StringColumn* sc = new StringColumn(2, s1, s2);
    CS4500_ASSERT_TRUE(sc -> as_string() == sc);

    delete bc;
    delete ic;
    delete fc;
    delete sc;

    exit(0);

}

TEST(W1, test4) {
    CS4500_ASSERT_EXIT_ZERO(test4)
}

//testing push_back
void test5() {
    BoolColumn* bc = new BoolColumn(4, 0, 1, 0, 0);
    bc -> push_back(1);
    CS4500_ASSERT_TRUE(bc -> get(4) == 1);

    IntColumn* ic = new IntColumn(4, 6, 8, 9, -4);
    ic -> push_back(-8);
    CS4500_ASSERT_TRUE(ic -> get(4) == -8);

    FloatColumn* fc = new FloatColumn(0);
    fc -> push_back(2.8);
    CS4500_ASSERT_TRUE((float_eq(fc -> get(0), 2.8)));

    FloatColumn* fc2 = new FloatColumn(2, 1.2, -8.3);
    fc2 -> push_back(4.2);
    CS4500_ASSERT_TRUE((float_eq(fc2 -> get(2), 4.2)));

    String* s1 = new String("hello");
    String* s2 = new String("world");
    String* s3 = new String("lalala");
    StringColumn* sc = new StringColumn(2, s1, s2);
    sc -> push_back(s3);
    CS4500_ASSERT_TRUE(s3 -> equals(sc -> get(2)));

    delete bc;
    delete ic;
    delete fc;
    delete fc2;
    delete sc;

    exit(0);
}
TEST(W1, test5) {
    CS4500_ASSERT_EXIT_ZERO(test5)
}

//testing size method
void test6() {
    BoolColumn* bc = new BoolColumn(4, 0, 1, 0, 0);
    CS4500_ASSERT_TRUE(bc -> size() == 4);

    IntColumn* ic = new IntColumn(4, 6, 8, 9, -4);
    CS4500_ASSERT_TRUE(ic -> size() == 4);

    FloatColumn* fc = new FloatColumn(0);
    FloatColumn* fc2 = new FloatColumn(1, 2.8);
    CS4500_ASSERT_TRUE(fc -> size() == 0);
    CS4500_ASSERT_TRUE(fc2 -> size() == 1);

    String* s1 = new String("hello");
    String* s2 = new String("world");
    StringColumn* sc = new StringColumn(2, s1, s2);
    CS4500_ASSERT_TRUE(sc -> size() == 2);

    delete bc;
    delete ic;
    delete fc;
    delete fc2;
    delete sc;

    exit(0);
}
TEST(W1, test6) {
    CS4500_ASSERT_EXIT_ZERO(test6)
}

//testing get type
void test7() {
    BoolColumn* bc = new BoolColumn(4, 0, 1, 0, 0);
    CS4500_ASSERT_TRUE(bc -> get_type() == 'B');

    IntColumn* ic = new IntColumn(4, 6, 8, 9, -4);
    CS4500_ASSERT_TRUE(ic -> get_type() == 'I');

    FloatColumn* fc = new FloatColumn(0);
    FloatColumn* fc2 = new FloatColumn(1, 2.8);
    CS4500_ASSERT_TRUE(fc -> get_type() == 'F');
    CS4500_ASSERT_TRUE(fc2 -> get_type() == 'F');

    String* s1 = new String("hello");
    String* s2 = new String("world");
    StringColumn* sc = new StringColumn(2, s1, s2);
    CS4500_ASSERT_TRUE(sc -> get_type() == 'S');

    delete bc;
    delete ic;
    delete fc;
    delete fc2;
    delete sc;

    exit(0);
}
TEST(W1, test7) {
    CS4500_ASSERT_EXIT_ZERO(test7);
}

// test serialize and deserialize
void test8() {
    BoolColumn* bc = new BoolColumn(4, 0, 1, 0, 0);
    IntColumn* ic = new IntColumn(4, 6, 8, 9, -4);
    FloatColumn* fc = new FloatColumn(3, 1, -4.9, 2.8);
    String* s1 = new String("]hello\n }");
    String* s2 = new String("world\\");
    StringColumn* sc = new StringColumn(2, s1, s2);

    // test serialization
    char* bs = bc->serialize();
    //printf("%s\n", bs);
    check(streq(bs, "[0 1 0 0]"), "BoolColumn serialization failed");
    
    char* is = ic->serialize();
    //printf("%s\n", is);
    check(streq(is, "[6 8 9 -4]"), "IntColumn serialization failed");
    
    char* fs = fc->serialize();
    //printf("%s\n", fs);
    //check(streq(fs, "[1 -4.9 2.8]"), "FloatColumn serialization failed");
    
    char* ss = sc->serialize();
    //printf("%s\n", ss);
    check(streq(ss, "[\\]hello\\\n\\ \\} world\\\\]"), "StringColumn serialization failed");

    // test deserialization
    BoolColumn* bd = dynamic_cast<BoolColumn*>(Column::deserialize(bs, 'B', bc->size()));
    check(bd != nullptr, "Cast failed");
    for (int i = 0; i < bc->size(); ++i) check(bd->get(i) == bc->get(i), "B Mismatched values");

    IntColumn* id = dynamic_cast<IntColumn*>(Column::deserialize(is, 'I', ic->size()));
    check(id != nullptr, "Cast failed");
    for (int i = 0; i < ic->size(); ++i) check(id->get(i) == ic->get(i), "I Mismatched values");

    FloatColumn* fd = dynamic_cast<FloatColumn*>(Column::deserialize(fs, 'F', fc->size()));
    check(fd != nullptr, "Cast failed");
    for (int i = 0; i < fc->size(); ++i) check(float_eq(fd->get(i), fc->get(i)), "F Mismatched values");

    StringColumn* sd = dynamic_cast<StringColumn*>(Column::deserialize(ss, 'S', sc->size()));
    check(sd != nullptr, "Cast failed");
    for (int i = 0; i < sc->size(); ++i) check(sd->get(i)->equals(sc->get(i)), "S Mismatched values");

    delete bc;
    delete ic;
    delete fc;
    delete sc;
    exit(0);
}
TEST(W1, test8) {
    CS4500_ASSERT_EXIT_ZERO(test8);
}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
