// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include <gtest/gtest.h>
#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../column.h"

#define CS4500_ASSERT_TRUE(a)  \
    ASSERT_EQ((a),true);
#define CS4500_ASSERT_FALSE(a) \
    ASSERT_EQ((a),false);
#define CS4500_ASSERT_EXIT_ZERO(a)  \
    ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*");

// This file tests the Rower class

void test1() {
    // TODO
    exit(0);
}

TEST(W1, test1) {
  
    CS4500_ASSERT_EXIT_ZERO(test1)
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
