// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#pragma once

#include "key.h"
#include "kd_map.h"
#include "kv_store.h"
#include "../../util/object.h"
#include "../../util/string.h"
#include "../../util/helper.h"
#include "../../util/thread.h"
#include "../dataframe/dataframe.h"
#include "../../application/application.h"
#include "../../networking/node.h"

// class declaration can be found in kv_store.h
// used to resolve circular dependencies

// per-node (per-application) KV Storage
// constructs an empty KVStore
KVStore::KVStore(const char* addr) {
    kdm_ = new KDMap();
    kdm_lock_ = new Lock();
    node_ = new Node(addr, this);
    idx_ = node_->start();
    deleted_ = false;
}

// deconstructor
KVStore::~KVStore() {
    delete node_;
    delete kdm_;
    delete kdm_lock_;
}

// tears down entire KVStore
void KVStore::teardown() {
    node_->teardown_all();
}

// waits for the index to be set and then gets it
int KVStore::get_idx() {
    if (idx_ >= 0) return idx_;
    else {
        while (idx_ < 0) kdm_lock_->wait();
        kdm_lock_->unlock();
        return idx_;
    }
}

// puts the given Key and DataFrame into this KVStore
// this method should only be called by KVStore class
void KVStore::put(Key* k, DataFrame* v) {
    if (k->idx_ != idx_) node_->put(k, v);
    else {
        kdm_lock_->lock();
        kdm_->put(k, v);
        kdm_lock_->unlock();
        kdm_lock_->notify_all();
    }
}

// gets the DataFrame for the given Key
// returns nullptr if the key does not exist in this store
DataFrame* KVStore::get(Key* k) {
    // call node get if index does not match this node's index
    if (k->idx_ != idx_) return node_->get(k);
    else {    
        kdm_lock_->lock();
        DataFrame* out = kdm_->get(k);
        kdm_lock_->unlock();
        return out;
    }
}

// waits until the given key is in this store, then returns the corresponding DataFrame
DataFrame* KVStore::wait_and_get(Key* k) {
    check(! deleted_, "All keys and vals were deleted_");
    if (k->idx_ != idx_) return node_->wait_and_get(k);
    else {
        kdm_lock_->lock();
        while(! kdm_->contains_key(k)) {
            // once the wait finishes, lock is acquired
            // gets notified every put
            kdm_lock_->wait();
        }
        kdm_lock_->unlock();
        DataFrame* out = get(k);
        // needs to unlock since wait acquires lock
        return out;
    }
}

// gets the number of local keys in this KVStore
size_t KVStore::local_size() {
    kdm_lock_->lock();
    size_t out = kdm_->size();
    kdm_lock_->unlock();
    return out;
}

// deletes all the keys and values in the whole KVStore (not just for this node's store
void KVStore::delete_all() {
    if (! deleted_) kdm_->delete_all();
    deleted_ = 1;
}
