// Authors: Zoe Corning (corning.z@husky.neu.edu) & Rucha Khanolkar (khanolkar.r@husky.neu.edu)
#include "../application.h"
#include "../../data/sorer/sorer.h"
#include "../../data/dataframe/rower.h"
#include "../../data/dataframe/dataframe.h"
#include "../../data/dataframe/split.h"
#include "../../util/set.h"
#include "../../util/helper.h"
#include "../../data/kv_store/kvs_impl.h"

// this class updates the given project sets with new projects based on new users
class ProjectFinder : public Rower {
    public:
        // references external
        Set* pSet_; // set of all projects found
        Set* new_projs_; // set of new projects just found
        Set* new_users_; // set of new users
        
        // constructor that takes in the project set to update and the new users to look for
        ProjectFinder(Set* pSet, Set* new_projs, Set* new_users) : Rower() {
            pSet_ = pSet;
            new_projs_ = new_projs;
            new_projs_->clear();
            new_users_ = new_users;
        }

        // deconstructor - data is external
        ~ProjectFinder() {} 

        // accepts the given commit and updates sets with new data
        bool accept(Row& r) {
            // ignores rows that are out of bounds
            if (r.get_int(1) > (int)(new_users_->max_) || r.get_int(2) > (int)(new_users_->max_) || 
                    r.get_int(0) > (int)(new_projs_->max_)) return false;
            // if the first user on the commit is in the new_users set
            if (new_users_->contains(r.get_int(1))) {
                // add the commit's project to the new_projects set if it's really new (not in pSet)
                int p = r.get_int(0);
                if (! pSet_->contains(p)) {
                    new_projs_->add(r.get_int(0));
                    pSet_->add(r.get_int(0));
                }
            }
            return true;
        }
};

// this class updates the given user sets with new users based on new users
class UserFinder : public Rower {
    public:
        // references external
        Set* uSet_; // set of all users found
        Set* new_users_; // set of new users just found
        Set* new_projs_; // set of new projects
        
        // constructor that takes in the user set to update and the new users to look for
        UserFinder(Set* uSet, Set* new_users, Set* new_projs) : Rower() {
            uSet_ = uSet;
            new_users_ = new_users;
            new_users_->clear();
            new_projs_ = new_projs;
        }

        // deconstructor - data is external
        ~UserFinder() {} 

        // accepts the given commit and updates sets with new data
        bool accept(Row& r) {
            // ignores rows that have out of bounds values
            if (r.get_int(1) > (int)(new_users_->max_) || r.get_int(2) > (int)(new_users_->max_) || 
                    r.get_int(0) > (int)(new_projs_->max_)) return false;
            // if the project in the commit is a new project
            if (new_projs_->contains(r.get_int(0))) {
                // if user 1 is not already tagged, add them
                int u1 = r.get_int(1);
                if (! uSet_->contains(u1)) {
                        new_users_->add(u1);
                        uSet_->add(u1);
                }
            }
            return true;
        }
};

// This application computes the collaborators of Linus Torvalds
class Linus : public Application {
    public: 
        const int DEG = 4; // degrees of separation from linus
        const int LUID = 4967; // Linus's user ID for big files
        const char* PROJ = "datasets/big/projects.ltgt";
        const char* USER = "datasets/big/users.ltgt";
        const char* COMM = "datasets/big/commits.ltgt";
        DataFrame* commits;  // pid x uid x uid
        Set* uSet; // Linus' collaborators
        Set* pSet; // projects of collaborators

        int num_nodes_; // including node 0
        int step_;
        Set* new_users; // new users from previous round
        Set* new_projs; // new projects

        // takes in address of this Linus node and the total number of nodes
        Linus(char* addr, int num_nodes) : Application(addr) {
            num_nodes_ = num_nodes;
        }

        // deconstructor
        ~Linus() {
            // commits deleted by KVStore in teardown
            delete uSet;
            delete pSet;
            delete new_users;
            delete new_projs;
        }

        void run() override {
            if (this_node() == 0) read_files_(); // node 0 reads file
            else setup_data_(); // nodes wait for data from node 0
            for (step_ = 0; step_ < DEG; ++step_) step_once_();
            if (this_node() == 0) finish_();
        }

        // splits up the commits DataFrame
        // and puts each resulting DF in correct node KVStore
        // assumes commits and num_nodes fields are set
        // Note: this is a helper for Node 0
        void split_commits_() {
            // split up commits and put each node's commits into KVStore
            DataFrame** node_comms = split_by_row(commits, num_nodes_ - 1);
            delete commits; // not necessary in Node 0
            for (int i = 1; i < num_nodes_; ++i) {
                // send all nodes their commits
                Key* k = new Key("comms", i); // store node_comms in correct node
                kvs_->put(k, node_comms[i-1]);
                delete k; // can delete, not stored locally
                delete node_comms[i-1];
            }
            delete[] node_comms;
            commits = nullptr;
        }

        // Node 0 reads data from three files (projects, users, commits)
        void read_files_() {
            Key* k;

            puts("Node 0: starting to read projects");
            DataFrame* projects = interpret_file(PROJ, 0, 0);
            int num_projects = projects->nrows();
            delete projects; // don't need to store whole projects, just pid in pSet
            printf("Node 0: finished reading projects - %d projects total\n", num_projects);
            pSet = new Set(num_projects);
            new_projs = new Set(num_projects);
            // put num_projects into KVStore
            DataFrame* np = DataFrame::from_scalar(num_projects);
            k = new Key("n_proj", 0);
            kvs_->put(k, np); // don't delete, stored locally

            puts("Node 0: starting to read users");
            DataFrame* users = interpret_file(USER, 0, 0);
            int num_users = users->nrows();
            delete users; // don't need to store all user data, just uids in uSet
            printf("Node 0: finished reading users - %d users total\n", num_users);
            uSet = new Set(num_users);
            new_users = new Set(num_users);
            new_users->add(LUID); // add Linus as only new user
            // put num_users into KVStore
            DataFrame* nu = DataFrame::from_scalar(num_users);
            k = new Key("n_user", 0);
            kvs_->put(k, nu); // don't delete, stored locally

            puts("Node 0: starting to read commits");
            commits = interpret_file(COMM, 0, 0);
            puts("Node 0: finished reading commits");
            split_commits_();
        }

        // Nodes > 0 wait for the data from Node 0, set up fields once data is received
        void setup_data_() {
            // 1. Nodes > 0 wait for num_projects - create a set of that size (bitmap style)
            Key* k = new Key("n_proj", 0);
            DataFrame* np = kvs_->wait_and_get(k);
            int num_projects = np->get_int(0, 0);
            pSet = new Set(num_projects);
            new_projs = new Set(num_projects);
            delete k;
            delete np;
            // 2. Nodes > 0 wait for num_users - create a set of that size "
            k = new Key("n_user", 0);
            DataFrame* nu = kvs_->wait_and_get(k);
            int num_users = nu->get_int(0, 0);
            uSet = new Set(num_users);
            new_users = new Set(num_users);
            delete k;
            delete nu;
            // 3. Nodes > 0 wait for commits chunk + locally save chunk
            k = new Key("comms", this_node());
            commits = kvs_->wait_and_get(k);
            delete k;
        }

        // converts a set into a dataframe (set elements all go in column 0)
        DataFrame* set_to_df_(Set* set) {
            int* e = set->elements();
            DataFrame* out = DataFrame::from_array(set->size(), e);
            delete[] e;
            return out;
        }

        // adds the dataframe's elements to the given set
        void update_set_(DataFrame* df, Set* set) {
            for (size_t i = 0; i < df->nrows(); ++i) set->add(df->get_int(0, i));
        }

        // sends out list of new users, then merges resulting new projects sets
        // called by node 0
        void merge_new_projs_() {
            // 1. Node 0 sends list of newly added users from previous round to all nodes
            DataFrame* nu = set_to_df_(new_users);
            Key* k = Key::make_key("nu-", step_, 0);
            kvs_->put(k, nu); // don't delete key or df (stored locally)
            // 2. Node 0 combines list of new projects from all nodes into set
            new_projs->clear();
            DataFrame* np;
            for (int i = 1; i < num_nodes_; ++i) {
                k = Key::make_key("np-", step_, i);
                np = kvs_->wait_and_get(k);
                update_set_(np, new_projs);
                update_set_(np, pSet);
                delete k; // not local
                delete np;
            }
            printf("Node 0: got new projects from all nodes - %lu new projects\n", new_projs->size());
        }

        // sends out list of new projects, then merges resulting new user sets
        // called by node 0
        void merge_new_users_() {
            // 3. Node 0 sends set of new projects to all nodes
            DataFrame* np = set_to_df_(new_projs);
            Key* k = Key::make_key("np-", step_, 0);
            kvs_->put(k, np); // don't delete, stored locally
            // 4. Node 0 combines lists of new users from all nodes into set
            new_users->clear();
            DataFrame* nu;
            for (int i = 1; i < num_nodes_; ++i) {
                k = Key::make_key("nu-", step_, i);
                nu = kvs_->wait_and_get(k);
                update_set_(nu, new_users);
                update_set_(nu, uSet);
                delete k;
                delete nu;
            }
            printf("Node 0: got new users from all nodes - %lu new users\n", new_users->size());
        }

        // calculates new projects
        // called by nodes > 0
        void calc_new_projs_() {
            // 1. Nodes > 0 wait for DF of newly added users
            Key* k = Key::make_key("nu-", step_, 0);
            DataFrame* nu = kvs_->wait_and_get(k);
            //    - Update local users set to tag new users
            new_users->clear();
            update_set_(nu, new_users);
            update_set_(nu, uSet);
            delete k; // delete - data stored on node 0
            delete nu;
            // 2. Nodes > 0 go through their commits + build up list of new projects
            ProjectFinder* pf = new ProjectFinder(pSet, new_projs, new_users);
            printf("Node %d: started looking for new projects\n", this_node());
            commits->map(*pf);
            printf("Node %d: finished looking for new projects\n", this_node());
            delete pf;
            // 3. Nodes > 0 send new projects to Node 0
            DataFrame* np = set_to_df_(new_projs);
            k = Key::make_key("np-", step_, this_node());
            kvs_->put(k, np); // don't delete key or df (stored locally)
        }

        // calculates new users
        // called by nodes > 0
        void calc_new_users_() {
            // 4. Nodes > 0 wait for set of new projects
            Key* k = Key::make_key("np-", step_, 0);
            DataFrame* np = kvs_->wait_and_get(k);
            //    - Update local projects set to tag new projects
            new_projs->clear();
            update_set_(np, new_projs);
            update_set_(np, pSet);
            delete k; // delete - data stored on 0
            delete np;
            // 5. Nodes > 0 go through commits + build up list of new users
            //    (that worked on new projects)
            UserFinder* uf = new UserFinder(uSet, new_users, new_projs);
            printf("Node %d: stared looking for new users\n", this_node());
            commits->map(*uf);
            printf("Node %d: finished looking for new users\n", this_node());
            delete uf;
            // 6. Nodes > 0 send list of new users to Node 0
            DataFrame* nu = set_to_df_(new_users);
            k = Key::make_key("nu-", step_, this_node());
            kvs_->put(k, nu); // don't delete (stored locally)
        }

        // Tags users in next degree from Linus
        void step_once_() {
            printf("Node %d: starting step %d\n", this_node(), step_);
            if (this_node() == 0) {
                merge_new_projs_();
                merge_new_users_();
            } else {
                calc_new_projs_();
                calc_new_users_();
            }
        }

        // does the last steps of this application (such as printing out answer and network teardown)
        void finish_() {
            kvs_->teardown();
            printf("SUCCESS: %lu USERS WITHIN %d DEGREES OF LINUS\n", uSet->size(), DEG);
        }
};

// Usage: ./linus_node <addr> <num_nodes>
int main(int argc, char** argv) {
    check(argc == 3, "Usage: ./linus_node <addr> <num_nodes>\n");

    Linus* l = new Linus(argv[1], atoi(argv[2]));
    l->run();

    delete l;
    
    return 0;
}
