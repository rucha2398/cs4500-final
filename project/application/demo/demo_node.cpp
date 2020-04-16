#include "../application.h"
#include "../../data/kv_store/kv_store.h"
#include "../../data/kv_store/key.h"
#include "../../util/helper.h"
#include "../../data/dataframe/dataframe.h"
#include "../../data/kv_store/kvs_impl.h"
#include <stdio.h>

// changed to 10,000 since our DataFrame only supports float/int numbers which are not large enough
// to hold the sum from 1 to 100,000 (approx. 5 billion)
//size_t SZ = 10*1000;
size_t SZ = 10*1000;

// code from Project Milestone 1 - adapted to fit our APIs
class Demo : public Application {
    public:
        Key* main = new Key("main", 0);
        Key* verify = new Key("verif", 1);
        Key* check = new Key("ck", 2);

        Demo(const char* addr) : Application(addr) {}

        void run() override {
            switch(this_node()) {
                case 0: producer(); break;
                case 1: counter(); break;
                case 2: summarizer();
            }
        }

        void producer() {
            puts("starting producer");
            int* vals = new int[SZ];
            int sum = 0;
            for (size_t i = 0; i < SZ; ++i) {
                sum += i;
                vals[i] = i;
            }
            DataFrame* df1 = DataFrame::from_array(SZ, vals);
            delete[] vals;
            kvs_->put(main, df1); // local
            DataFrame* df2 = DataFrame::from_scalar(sum);
            kvs_->put(check, df2);
            delete check; // non-local
            delete verify; // non-local
            delete df2; // non-local
            puts("producer done");
        }

        void counter() {
            puts("starting counter");
            DataFrame* v = kvs_->wait_and_get(main);
            delete main; // non-local
            int sum = 0;
            for (size_t i = 0; i < SZ; ++i) sum += v->get_int(0, i);
            delete v; // non-local
            p("The sum is ");
            pln(sum);
            DataFrame* df = DataFrame::from_scalar(sum);
            kvs_->put(verify, df); // local
            puts("counter done");
            delete check; // non-local
        }

        void summarizer() {
            puts("starting summarizer");
            DataFrame* result = kvs_->wait_and_get(verify);
            delete verify; // non-local
            DataFrame* expected = kvs_->wait_and_get(check); // local
            pln(expected->get_int(0, 0) == result->get_int(0, 0) ? "SUCCESS":"FAILURE");
            delete result; // non-local
            delete main; 
            // clean up memory after application finishes
            kvs_->teardown(); 
            delete check;
        }
};

// Usage: ./demo_node 127.0.0.0
int main(int argc, char** argv) {
    check(argc == 2, "Usage: ./demo_node <ip>");
    
    Demo* d = new Demo(argv[1]);
    d->run();

    delete d;

    return 0;
}
