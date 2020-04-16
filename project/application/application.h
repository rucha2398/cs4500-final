// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#pragma once

#include "../data/kv_store/kv_store.h"
#include "../util/helper.h"
#include "../util/object.h"

// This is a single node in the distributed system
class Application: public Object {
    public:
        KVStore* kvs_; // this node's kv store - owned
        int idx_; // index of this node

        // constructs this node using the given index
        Application(const char* addr) : Object() {
            kvs_ = new KVStore(addr); // sets up KVStore
            idx_ = kvs_->get_idx(); // unknown index until KVStore is setup
        }

        // deconstructor - handles deleting the KVStore for this node
        ~Application() {
            delete kvs_;
        }

        // run method - should be overwritten in child application
        virtual void run() { check(false, "Run can't be called on parent Application class"); }

        // returns the index of this node
        int this_node() { return idx_; }
};
