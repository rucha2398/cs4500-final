// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#pragma once

#include "../../util/string.h"
#include "../../util/object.h"
#include "../../util/helper.h"

// key class used for Distributed Key-Value Storage
class Key : public Object {
    public:
        char* str_; // string of the key - String is owned
        int idx_; // index of the node that owns this data
        
        // constructor
        Key(const char* string, int index) : Object() {
            str_ = duplicate(string);
            check(index >= 0, "Index of key cannot be negative");
            idx_ = index;
        }

        // deconstructor - deletes str_ since it is copied in the constructor
        ~Key() {
            delete[] str_;
        }

        // determines if this key is equal to the given object
        bool equals(Object* other) { 
            Key* ok = dynamic_cast<Key*>(other);
            return ok != nullptr && streq(ok->str_, str_) && ok->idx_ == idx_;
        }

        // hashes this key
        size_t hash_me() {
            size_t hash = 0;
            for (size_t i = 0; i < strlen(str_); ++i) {
                hash += str_[i];
            }
            hash += idx_;
            return hash;
        }

        // serializes the given key into the following format:
        // <str_> <idx_>
        char* serialize() {
            StrBuff* sb = new StrBuff();
            char* tmp = duplicate(str_);
            char to_esc[] = {ESC, '\n', DLM, '|', ']', '}'};
            char* esc_tmp = add_escapes(tmp, to_esc);
            delete[] tmp;
            sb->c(esc_tmp);
            delete[] esc_tmp;
            sb->c(DLM);
            sb->c(idx_);

            char* out = sb->no_cpy_get();
            delete sb;
            return out;
        }

        // deserializes the given string into a Key
        static Key* deserialize(char* m) {
            char* rest = nullptr;
            char* str = next_token(m, &rest, DLM, true);
            char* tok = next_token(rest, &rest, DLM, false);
            size_t idx = atoi(tok);
            delete[] tok;

            Key* out = new Key(str, idx);
            
            delete[] str;
            return out;
        }

        // creates a key by combining the str and idx_s to make the string of the key
        // ex. make_key("key-", 1, 0) returns Key:{str="key-1", idx=0}
        static Key* make_key(const char* str, int idx_s, int idx) {
            StrBuff* sb = new StrBuff();
            sb->c(str);
            sb->c(idx_s);
            char* k_str = sb->no_cpy_get();
            delete sb;

            Key* out = new Key(k_str, idx);
            delete[] k_str;
            return out;
        }
};
