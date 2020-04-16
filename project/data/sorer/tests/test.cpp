// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include "../sorer.h"
#include "../../dataframe/column.h"
#include "../../dataframe/schema.h"
#include "../../dataframe/dataframe.h"
#include "../../../util/string.h"
#include "../../../util/helper.h"

// tests on 0.sor
void test0() {
    const char* msg = "Test 0 Failed";
    DataFrame* df = interpret_file("0.sor", 0, 0);
    
    check(df->get_schema().col_type(0) == 'B', msg);
    check(df->get_bool(0, 0) == 0, msg); // default for missing boolean is 0

    delete df;
    
    puts("Test 0 Passed");
}

// tests on 1.sor
void test1() {
    const char* msg = "Test 1 Failed";
    DataFrame* df = interpret_file("1.sor", 0, 0);
    
    check(df->get_schema().col_type(0) == 'S', msg);
    check(df->get_string(0, 0)->equals("abc"), msg);
    check(df->get_string(0, 3)->equals("+1"), msg);

    // with from and len arguments
    DataFrame* df2 = interpret_file("1.sor", 1, 74);
    
    check(df2->get_schema().col_type(0) == 'S', msg);
    check(df2->get_string(0, 0)->equals("1"), msg);
    check(df2->get_string(0, 6)->equals("+2.2"), msg);

    delete df;
    delete df2;
    
    puts("Test 1 Passed");
}

void test2() {
    const char* msg = "Test 2 Failed";
    DataFrame* df = interpret_file("2.sor", 0, 0);

    check(df->get_schema().col_type(0) == 'B', msg);
    check(df->get_schema().col_type(1) == 'I', msg);
    check(df->get_schema().col_type(2) == 'F', msg);
    check(df->get_schema().col_type(3) == 'S', msg);
    check(df->get_int(1, 0) == 0, msg); // missing int = 0
    check(df->get_int(1, 1) == 12, msg);
    check(df->get_string(3, 0)->equals("hi"), msg);
    check(df->get_string(3, 1)->equals("ho ho ho"), msg);
    check(float_eq(df->get_float(2, 0), 1.2), msg);
    check(float_eq(df->get_float(2, 1), -0.2), msg);

    delete df;

    puts("Test 2 Passed");
}

//tests on 3.sor
void test3() {
    const char* msg = "Test 3 Failed";
    DataFrame* df = interpret_file("3.sor", 0, 0);

    check(df->get_schema().col_type(4) == 'B', msg);
    check(float_eq(df->get_float(2, 10), 1.2), msg);
    check(float_eq(df->get_float(2, 384200), 1.2), msg);

    delete df;

    puts("Test 3 Passed");
}

//tests on 4.sor
void test4() {
    const char* msg="Test 4 Failed";
    DataFrame* df = interpret_file("4.sor", 0, 0);

    check(df->get_int(0, 1) == 2147483647, msg);
    check(df->get_int(0, 2) == -2147483648, msg);
    check(float_eq(df->get_float(1, 1), -0.000000002), msg);
    check(float_eq(df->get_float(1, 2), 10000000000), msg);

    delete df;
    
    puts("Test 4 Passed");
}

int main() {
    test0();
    test1();
    test2();
    // test3(); 3.sor does not fit into size limit on handin server
    // TODO to run this test, redownload from 3.sor piazza post
    test4();

    return 0;
}
