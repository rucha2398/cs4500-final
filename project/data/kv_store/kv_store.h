// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#pragma once

#include "../../util/object.h"
#include "../../util/string.h"
#include "../../util/helper.h"
#include "../../util/thread.h"
#include "../dataframe/dataframe.h"
#include "key.h"
#include "kd_map.h"

class Node;

// implementation can be found in kvs_impl.h
// needed to separated declaration and implementation to resolve circular dependencies with node 

// per-node (per-application) KV Storage
// forward declaration - bodies of functions declared below KVStore
class KVStore : public Object {
    public:
        Node* node_;
        KDMap* kdm_; // maps the keys for this node to this node's data (i.e. Key->idx_ == idx_)
        Lock* kdm_lock_; // lock for the kdmap
        int idx_; // index of the node that owns this store
        bool deleted_; // flag that is set to true if delete_all() is called

        // constructs an empty KVStore
        KVStore(const char* addr);

        // deconstructor
        ~KVStore(); 

        // tears down the entire KVStore
        void teardown();

        // waits until set_idx is called then returns this KVStore's index
        int get_idx();

        // puts the given Key and DataFrame into this KVStore
        // this method should only be called by KVStore class
        void put(Key* k, DataFrame* v);
        
        // gets the DataFrame for the given Key
        // key index must match this index
        // returns nullptr if the key does not exist in this store
        DataFrame* get(Key* k);

        // waits until the given key is in this store, then returns the corresponding DataFrame
        DataFrame* wait_and_get(Key* k);
       
        // gets the number of local keys in this KVStore
        size_t local_size();

        // deletes all the keys and values in the whole KVStore (not just for this node's store
        void delete_all();
};
