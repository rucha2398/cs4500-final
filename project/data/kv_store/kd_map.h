// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#pragma once

#include "key.h"
#include "../../util/helper.h"
#include "../../util/object.h"
#include "../dataframe/dataframe.h"

// class used to store one key, dataframe pair
class MapPair : public Object {
    public:
        Key* key_; // not owned, immutable
        DataFrame* val_; // not owned
        bool tomb_; // true if this pair has been removed from map

        // constructor for a new map pair
        // @pre key and val cannot be null
        MapPair(Key* k, DataFrame* v) : Object() {
            check(k != nullptr, "Key cannot be null");
            check(v != nullptr, "Val cannot be null");
            key_ = k;
            val_ = v;
            tomb_ = 0;
        }

        ~MapPair() {} // data is not owned, so it is not deleted here

        // sets this pair's value to the given calue
        // @return the overwritten value
        DataFrame* set_val(DataFrame* v) {
            DataFrame* out = val_;
            val_ = v;
            return out;
        }

        // returns true if this pair's key equals the given key
        bool same_key(Key* k) { return key_->equals(k); }
            
        // returns the hash of this pair's key
        size_t hash_key() { return key_->hash(); }
};

// map from Key* to DataFrame*
// map is a hash map that uses linear probing for efficiency
class KDMap : public Object {
    public:
        MapPair** pairs_; // array and pairs are owned, but not keys/dataframes
        size_t size_; // number of key/value pairs in this map
        size_t cap_; // capacity of the array used to store the map data

        // creates an empty map
        KDMap() : Object() {
            size_ = 0;
            cap_ = 4; // Default cap = 4
            pairs_ = new MapPair*[cap_];
            memset(pairs_, 0, sizeof(MapPair*) * cap_);
        }

        // deconstructor
        ~KDMap() {
            // pairs are owned, but keys and values are not
            for(size_t i=0; i<cap_; ++i) {
                delete pairs_[i];
            }
            delete[] pairs_;
        }

        // returns the number of pairs in this map
        size_t size() { return size_; }

        // this is a PRIVATE helper method to find the index of the given key
        // if the index cannot be found, retuns cap_ + 1
        size_t index_of_(Key* key) {
            for (size_t i = 0; i < cap_; ++i) {
                if (pairs_[i] != nullptr && pairs_[i]->same_key(key) && !pairs_[i]->tomb_) return i;
            }
            return cap_ + 1;
        }

        // this is a PRIVATE method to grow the size of the array if necessary
        void grow_() {
            MapPair** old_pairs = pairs_;
            size_t old_cap = cap_;
            cap_ *= 2;
            pairs_ = new MapPair*[cap_];
            memset(pairs_, 0, sizeof(MapPair*) * cap_);
            size_ = 0;

            for (size_t i = 0; i < old_cap; ++i) {
                if (old_pairs[i] != nullptr && !old_pairs[i]->tomb_) {
                    put_pair_(old_pairs[i]);
                }
            }

            delete[] old_pairs;
        }
   
        // this is a private helper method to put the given MapPair into this Map
        // if the key already exists in the map, then its value is replaced and returned
        // else the return value is nullptr
        DataFrame* put_pair_(MapPair* mp) {
            if (cap_/(size_ + 1) < 2) grow_();

            size_t idx = index_of_(mp->key_);
            // key is not already in the map
            if (idx > cap_) {
                size_t i = mp->hash_key() % cap_;
                bool placed = 0;
                while (!placed) {
                    MapPair* cur = pairs_[i];
                    if (cur == nullptr || cur->tomb_) {
                        if (cur != nullptr) delete cur;
                        pairs_[i] = mp;
                        placed = 1;
                    }
                    i = mod(i + 1, cap_);
                }
                ++size_;
                return nullptr;
            } else { // key is already in the map -> replace value
                DataFrame* out = pairs_[idx]->set_val(mp->val_);
                delete mp;
                return out;
            }
        }

        // puts the given key and value into this map
        // returns the overwritten value if the key already exists in the map, else nullptr
        DataFrame* put(Key* key, DataFrame* val) {
            // checks for non-null in MapPair constructor
            MapPair* mp = new MapPair(key, val);
            return put_pair_(mp);
        }

        // gets the dataframe for the given key
        DataFrame* get(Key* key) {
            size_t idx = index_of_(key);
            if (idx > cap_) return nullptr;
            else {
                return pairs_[idx]->val_;
            }
        }

        // returns true if this map contains the given key
        bool contains_key(Key* k) {
            return index_of_(k) < cap_;
        }

        // removes the given key/dataframe pair from this map
        DataFrame* remove(Key* key) {
            size_t idx = index_of_(key);
            if (idx > cap_) {
                return nullptr;
            } else {
                pairs_[idx]->tomb_ = 1;
                --size_;
                return pairs_[idx]->val_;
            }
        }

        // deletes all the keys and values in this map
        void delete_all() {
            for (size_t i = 0; i < cap_; ++i) {
                if (pairs_[i] != nullptr) {
                    delete pairs_[i]->key_;
                    delete pairs_[i]->val_;
                    delete pairs_[i];
                    pairs_[i] = nullptr;
                }
            }
        }
};
