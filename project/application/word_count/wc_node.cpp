// Authors: Zoe Corning (corning.z@husky.neu.edu) and Rucha Khanolkar (khanolkar.r@husky.neu.edu)

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include "../application.h"
#include "../../data/dataframe/dataframe.h"
#include "../../data/dataframe/column.h"
#include "../../util/helper.h"
#include "../../util/string.h"
#include "../../data/kv_store/kvs_impl.h"

/* Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
*/

class WordCount : public Application {
    public:
        const char* filename_;
        int fr_idx_ = 0; // index of file reader node
        int ctrs_; // index of counter nodes
        int r_idx_; // index of reduce node
        
        // creates a WordCount application with the given address and 
        // the given number of counter nodes
        WordCount(const char* addr, const char* filename, int counters) : Application(addr) { 
            ctrs_ = counters;
            r_idx_ = ctrs_ + 1;
            filename_ = filename;
        }

        // runs this word count application
        void run() override {
            if (this_node() == fr_idx_) {
                // read from a file - put in in for each node
                read_file();
            } else if (this_node() == r_idx_) {
                // reduce
                reduce();
            } else {
                // counter
                local_count();
            }
        }

        // reads the words from the file into an array of columns
        Column** read_words_() {
            const size_t buf_size = 1024;
            Column** cols = new Column*[ctrs_]; // columns to build up for each node
            for (int i = 0; i < ctrs_; ++i) cols[i] = new StringColumn();
            FILE* file = fopen(filename_, "r");
            char buf[buf_size];
            StrBuff* tmp = nullptr;
            int col = 0; // index of column to put next string into
            // while it hasn't gotten to the end of the file
            while (fgets(buf, buf_size, file) != nullptr) {
                char str[buf_size]; // array used to build up strings before they're added
                // i is the index into buf, si is the index into str
                // si resets to 0 when the built up string is added to the list
                for (size_t i = 0, si = 0; i <= strlen(buf); ++i) {
                    char c = tolower(buf[i]); // does nothing to non-capital letter chars
                    if (isalpha(c)) {
                        str[si] = c;
                        ++si;
                    } else {
                        // if we're at the end of the buffer
                        // and the word straddles to the next read
                        if (c == '\0' && i == buf_size - 1 && str[0] != '\0') {
                            str[si] = '\0';
                            // want to store word so far in tmp
                            if (tmp == nullptr) tmp = new StrBuff();
                            tmp->c(str);
                        // in this case, there is a non-letter char
                        } else {
                            // if there is a built up word, add it to list
                            if (str[0] != '\0') {
                                str[si] = '\0'; // null terminate chars
                                String* e = nullptr;
                                // the word is partial
                                if (tmp != nullptr) {
                                    tmp->c(str);
                                    e = new String(tmp->no_cpy_get());
                                    delete tmp;
                                    tmp = nullptr;
                                } else e = new String(str);
                                cols[col]->push_back(e);
                                col = mod(col + 1, ctrs_);
                            }
                        }

                        // reset str buffer
                        str[0] = '\0';
                        si = 0;
                    }
                }
            }
            check(feof(file), "Did not reach end of file");
            fclose(file);
            return cols;
        }

        // reads the given file and sorts the words so counters can count later
        // followed solution from Warmup 3
        void read_file() {
            puts("Node 0: starting to read file");
            Column** cols = read_words_();
            puts("read all the words");
            // put columns into KVStore for counters to start using
            char* k_str = new char[strlen("in_000")];
            for (int i = 0; i < ctrs_; ++i) {
                // wrap column in DataFrame
                Schema* s = new Schema(0, cols[i]->size());
                DataFrame* df = new DataFrame(*s);
                df->add_column(cols[i]);
                sprintf(k_str, "in_%d", i+1); // create key
                Key* k = new Key(k_str, fr_idx_);
                kvs_->put(k, df); // local
                delete s;
            }
            delete[] k_str;
            delete[] cols;
        }

        // waits for data and then counts the distinct words in this node's corresponding column
        void local_count() {
            // Key looks like this: {"in_<this_node>", fr_idx_}
            char* k_str = new char[strlen("in_000")];
            sprintf(k_str, "in_%d", this_node());
            Key* in_k = new Key(k_str, fr_idx_);
            // wait for file reader to put in data for this node
            DataFrame* words = kvs_->wait_and_get(in_k);
            delete in_k; // non-local
            
            printf("Node %d starting local count\n", this_node());
            for (size_t i = 0; i < words->nrows(); ++i) {
                // create key for current word: {<word> <this_node>}
                Key* ki = new Key(words->get_string(0, i)->c_str(), this_node());
                // get count for current word (will be nullptr if new word)
                DataFrame* count = kvs_->get(ki);
                // put in new key/val pair if new word
                if (count == nullptr) kvs_->put(ki, DataFrame::from_scalar(1));
                // else just increment the count in the dataframe
                else {
                    count->set(0, 0, count->get_int(0, 0) + 1);
                    delete ki;
                }
            }
             
            delete words; // non-local

            // puts count into the reduce node's store 
            // key looks like this: {"ct_<this_node>", r_idx_}
            sprintf(k_str, "ct_%d", this_node());
            Key* count_k = new Key(k_str, r_idx_);
            // NOTE: this does not consider case where words from different sections are not distinct
            DataFrame* tmp = DataFrame::from_scalar(static_cast<int>(kvs_->local_size()));
            kvs_->put(count_k, tmp);
            delete count_k; // non-local
            delete tmp; // non-local
            delete[] k_str;
            printf("Node %d done counting\n", this_node());
        }

        void reduce() {
            printf("Node %d reducing counts\n", r_idx_);
            int sum = 0;
            
            char k_str[strlen("ct_000")];
            // sum up data from all counters
            for (int i = 1; i <= ctrs_; ++i) {
                sprintf(k_str, "ct_%d", i);
                Key* k = new Key(k_str, this_node());
                DataFrame* count = kvs_->wait_and_get(k);
                sum += count->get_int(0, 0);
                delete k; // non-local
            }

            printf("SUCCESS: COUNT = %d\n", sum);
            
            // clean up network/KVStore after application finishes
            kvs_->teardown();
        }
};

// Usage: ./wc_node <ip> <filename> <num_counters>
int main(int argc, char** argv) {
    check(argc == 4, "Usage: ./wc_node <ip> <filename> <num_counters>");

    WordCount* wc = new WordCount(argv[1], argv[2], atoi(argv[3]));
    wc->run();

    delete wc;

    return 0;
}
