// Authors: Zoe Corning (corning.z@husky.neu.edu), Rucha Khanolkar (khanolkar.r@husky.neu.edu)
#include "../../../util/helper.h"
#include "../key.h"
#include "../kv_store.h"
#include "../kd_map.h"

// tests serialization and deserialization on Key
void testKeySer() {
    const char* str = "key name|with weird]} chars\n\\";
    Key* k = new Key(str, 2);
    
    char* ks = k->serialize();
    printf("%s\n", ks);
    check(streq(ks, "key\\ name\\|with\\ weird\\]\\}\\ chars\\\n\\\\ 2"), "Key serialization failed");

    Key* kd = Key::deserialize(ks);
    check(streq(kd->str_, k->str_), "Incorrect str");
    check(kd->idx_ == k->idx_, "Incorrect idx");

    delete k;
    delete[] ks;
    delete kd;

    puts("Test Key Serialization Passed");
}

int main() {
    testKeySer();
    return 0;
}
