// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <assert.h>
#include "../util/string.h"
#include "../util/object.h"
#include "../util/helper.h"
#include "network.h"

// class that handles setting up and usage of the server
class Server : public Object {
    public:
        size_t num_nodes_;
        Socket** nodes_; // ordered by consecutive index - owned

        // constructs a server with the given number of nodes
        Server(size_t num_nodes) {
            num_nodes_ = num_nodes;
        }

        // deletes node sockets
        ~Server() {
            for (size_t i = 0; i < num_nodes_; ++i) delete nodes_[i];
            delete[] nodes_;
        }

        // helper that creates a Directory with all the node's addresses in the same order of nodes_
        Directory* register_all_() {
            // get Register messages from all the nodes and put them in a Directory
            char** addresses = new char*[num_nodes_];
            for (size_t i = 0; i < num_nodes_; ++i) {
                Register* reg = dynamic_cast<Register*>(nodes_[i]->read_msg());
                check(reg != nullptr, "Server: Invalid Register message");
                addresses[i] = reg->get_node();
                delete reg;
            }
        
            // build the directory message
            // SIP used here as a placeholder - inside loop, use update_target to send to each node
            Directory* out = new Directory(SIDX, num_nodes_, addresses);
            for (size_t i = 0; i < num_nodes_; ++i) {
                delete[] addresses[i];
            }
            delete[] addresses;
            return out;
        }

        // sends the given message to all nodes connected to the server
        void broadcast_(Message* msg) {
            for (size_t i = 0; i < num_nodes_; ++i) {
                msg->update_target(i);
                nodes_[i]->send_msg(msg);
            }
        }

        // receives Open messages from all nodes 
        // - meaning all nodes are open for incoming connections
        void receive_opens_() {
            // starts from 1 since node 0 only sends outgoing connections
            for (size_t i = 1; i < num_nodes_; ++i) {
                Open* open = dynamic_cast<Open*>(nodes_[i]->read_msg());
                check(open != nullptr, "Server: Invalid Open message");
                delete open;
            }
        }
        
        // checks that all the sockets to the nodes are closed
        void check_closed_() {
            for (size_t i = 0; i < num_nodes_; ++i) {
                nodes_[i]->wait_for_close();
                printf("Server: node[%lu] closed\n", i);
            }
        }

        // runs the server
        void start() {
            // accept connections from all the nodes
            nodes_ = Socket::accept_n(SIP, PORT, num_nodes_);
            puts("Server: accepted all node connections");
             
            Directory* dir = register_all_();
            puts("Server: got all register messages");
            broadcast_(dir);
            puts("Server: sent directory to all nodes");
           
            // receive acknowledgements from all nodes
            receive_opens_();

            // send Connect message to node 0 to tell it it can start connecting to other nodes
            Connect* con = new Connect(0);
            nodes_[0]->send_msg(con);
            delete con;

            // wait for a node to initiate teardown
            int active_idx = select_active(num_nodes_, nodes_); // receive Kill
            Kill* k = dynamic_cast<Kill*>(nodes_[active_idx]->read_msg());
            check(k != nullptr, "Server: Invalid kill message");
            k->sender_ = SIDX;
            broadcast_(k); // broadcast kill message
            puts("Server: waiting for nodes to close");
            check_closed_(); // wait for all nodes to close server connection
            puts("Server: all node sockets closed");

            delete k;
            delete dir;
        }
};

// Usage: ./server <num_nodes>
int main(int argc, char** argv) {
    check(argc == 2, "Usage: ./server <num_nodes>");
    size_t num_nodes = atoi(argv[1]);
    Server* s = new Server(num_nodes);
    s->start();

    delete s;

    return 0;
}
