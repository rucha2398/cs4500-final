// Authors: Zoe Corning (corning.z@husky.neu.edu) & Rucha Khanolkar (khanolkar.r@husky.neu.edu)
#include "../node.h"
#include "../../data/kv_store/kv_store.h"
#include "../../data/kv_store/kvs_impl.h"

// Usage: ./node <node_addr>
int main(int argc, char** argv) {
    check(argc == 2, "Usage: ./node <node_addr>");
    KVStore* kvs = new KVStore(argv[1]);
    
    if (kvs->idx_ == 0) {
        kvs->teardown();
    }

    delete kvs;

    return 0;
}

