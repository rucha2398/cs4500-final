// Authors: Zoe Corning (corning.z@husky.neu.edu) & Rucha Khanolkar (khanolkar.r@husky.neu.edu)
#pragma once

#include "network.h"
#include "../util/helper.h"
#include "../util/thread.h"
#include "./serialization/message.h"
#include "../data/dataframe/dataframe.h"
#include "../data/kv_store/key.h"
#include "../data/kv_store/kv_store.h"

// thread used for wait and get
class WaitThread : public Thread {
    public:
        WaitGet* wg_; // message to handle once the wait finishes
        Socket* sock_; // socket to send response to
        KVStore* kvs_; // store to wait on
        bool done_; // set to true when response message is sent

        // creates a wait thread to wait for the response to the following message
        // Note: given message is now owned
        WaitThread(WaitGet* w, Socket* s, KVStore* kvs) : Thread() {
            wg_ = w;
            sock_ = s;
            kvs_ = kvs;
            done_ = 0;
        }

        // nothing is owned so no deletion in deconstructor
        ~WaitThread() {
            delete wg_->key_; 
            delete wg_;
        }
        
        // runs the thread
        void run() {
            // wait in kvstore
            DataFrame* df = kvs_->wait_and_get(wg_->key_);
            // create and send response message
            GetReply* r = new GetReply(wg_->sender_, wg_->key_, df);
            sock_->send_msg(r);
            delete r;
            done_ = true;
        }
};

// class that handles setting up and usage of a node in the network
class Node : public Object {
    public:
        size_t num_nodes_;
        Socket** nodes_; // ordered by consecutive index (this node's index is nullptr)
        Socket* serv_; // socket to server
        char addr_[IPLEN]; // address of this node: owned
        int idx_; // index of this node - set when Directory is recieved
        KVStore* kvs_;
        
        GetReply* reply_; // stores the most recent reply for get
        Lock* r_lock_; // lock for reply

        WaitThread** wts_; // threads waiting for wait_and_get - joined on teardown
        std::thread listener_;

        bool teardown_; // true if teardown is in progress
        
        // constructs a node with the given IP address
        Node(const char* addr, KVStore* kvs) {
            Message::copy_ip_(addr_, addr);
            idx_ = UIDX;
            reply_ = nullptr;
            r_lock_ = new Lock();
            kvs_ = kvs;
            teardown_ = false;
        }

        // deconstructor cleans up all threads and cleans up fields
        ~Node() {
            listener_.join();
            for (size_t i = 0; i < num_nodes_; ++i) {
                if (wts_[i] != nullptr) {
                    wts_[i]->join();
                    delete wts_[i];
                }
            }
            delete[] wts_;
            for (size_t i = 0; i < num_nodes_; ++i) delete nodes_[i];
            delete[] nodes_;
            delete serv_;
            delete r_lock_;
        }
    
        // constructor that constructs a node with a null kvs
        // FOR TESTING ONLY
        Node(const char* addr) : Node(addr, nullptr) { }
    
        // connects to and sends a register message to the server
        void serv_register_() {
            // create server socket and connect
            serv_ = Socket::connect_to_addr(SIP, PORT);
            printf("Node %d: connected to server\n", idx_);

            // send register message
            Register* reg = new Register(addr_);
            serv_->send_msg(reg);
            delete reg;
            printf("Node %d: sent register to server\n", idx_);
        }
    
        // receives the directory from the server and update's this node's information to match
        // the directory (ex. sets the index of this node, sets num_nodes)
        Directory* receive_directory_() {
            Directory* dir = dynamic_cast<Directory*>(serv_->read_msg());
            check(dir != nullptr, "Invalid Directory");
            printf("Node %d: received Directory\n", idx_);

            // figure out what this node's index is
            idx_ = dir->index_of(addr_);
            printf("*** My index is: %d ***\n", idx_);
            // set num_nodes based on directory's size
            num_nodes_ = dir->size();
            nodes_ = new Socket*[num_nodes_];
            wts_ = new WaitThread*[num_nodes_];
            memset(wts_, 0, num_nodes_ * sizeof(WaitThread*));
            // set this socket to nullptr
            nodes_[idx_] = nullptr;

            return dir;
        }

        // accepts incoming connections from all the nodes with a lower index than this node
        // sends Open message to server once this node is open for new connections
        void accept_incoming_() {
            // accepts idx_ number of connections because that's how many there are before
            // accept_n sends the open message to the server once the connection is open
            Open* open = new Open(idx_);
            Socket** in_conns = Socket::accept_n(addr_, PORT, idx_, serv_, open);
            delete open; // sent in accept_n
            printf("Node %d: accepted all incoming connections\n", idx_);
            // receive greeting message from nodes before in list to get their index
            for (int i = 0; i < idx_; ++i) {
                Greeting* g = dynamic_cast<Greeting*>(in_conns[i]->read_msg());
                nodes_[g->sender_] = in_conns[i];
                delete g;
            }
            printf("Node %d: received all greetings\n", idx_);
            delete[] in_conns;
        }

        // creates outgoing socket connections to all nodes in the network with a higher index
        // than this node
        // takes in a Directory messasge to look up the node's addresses
        void create_outgoing_(Directory* dir) {
            // waits for server's Connect message first if this node is node 0
            if (idx_ == 0) {
                Connect* con = dynamic_cast<Connect*>(serv_->read_msg());
                check(con != nullptr, "Invalid Connect message");
                printf("Node %d: received Connect message\n", idx_);
                delete con;
            }

            // connect to nodes with higher index than idx_
            // SIDX is a placeholder for node idxs -> will be replaced by update_target
            Greeting* g = new Greeting(idx_, SIDX);
            for (size_t i = idx_ + 1; i < num_nodes_; ++i) {
                nodes_[i] = Socket::connect_to_addr(dir->get(i), PORT);

                // send greeting message
                g->update_target(i);
                nodes_[i]->send_msg(g);
            }
            printf("Node %d: sent all greetings\n", idx_);
            delete g;
        }

        // handles all of the active sockets in this node (marked with active_ flag)
        void handle_active_() {
            for (size_t i = 0; i < num_nodes_; ++i) {
                if (i == (size_t)idx_) {
                    while (serv_->active_) handle_message_(serv_->read_msg());
                } else if (i != (size_t)idx_ && nodes_[i]->active_) {
                    while (nodes_[i]->active_) handle_message_(nodes_[i]->read_msg());
                }
            }
        }

        // handles the given message
        // given message is owned by this method
        void handle_message_(Message* m) {
            MsgKind k = m->kind_;
            if (m->sender_ == SIDX) { // message from the server
                check(k == MsgKind::Kill, "Node: Invalid Kill message");
                delete m;
                if (! teardown_) teardown_node_();
            } else { // message from another node
                if (k == MsgKind::Put) {
                    Put* p = dynamic_cast<Put*>(m);
                    check(p != nullptr, "Node: Cast failed");
                    check(p->key_->idx_ == idx_, "Node: Mismatched indices");
                    kvs_->put(p->key_, p->val_);
                    delete p; // don't want to delete key/val, stored locally
                } else if (k == MsgKind::Get) {
                    Get* g = dynamic_cast<Get*>(m);
                    check(g != nullptr, "Node: Cast failed");
                    check(g->key_->idx_ == idx_, "Node: Mismatched indices");
                    
                    GetReply* gr = new GetReply(g->sender_, g->key_, kvs_->get(g->key_));
                    send_to_node(gr);

                    delete g->key_;
                    delete g;
                    delete gr; // don't delete gr->key_, same as g->key_
                } else if (k == MsgKind::WaitGet) {
                    WaitGet* w = dynamic_cast<WaitGet*>(m);
                    check(w != nullptr, "Node: Cast failed");
                    check(w->key_->idx_ == idx_, "Node: Mismatched indices");
                    
                    int is = w->sender_;

                    if (wts_[is] == nullptr || wts_[is]->done_) {
                        if (wts_[is] != nullptr) {
                            wts_[is]->join();
                            delete wts_[is];
                            wts_[is] = nullptr;
                        }
                        wts_[is] = new WaitThread(w, nodes_[is], kvs_);
                        wts_[is]->start();
                    }
                    // else ignore message - node is already waiting for another key
                } else if (k == MsgKind::GetReply) {
                    GetReply* r = dynamic_cast<GetReply*>(m);
                    check(r != nullptr, "Node: Cast failed");
                    check(r->target_ == idx_, "Node: Mismatched indices");

                    r_lock_->lock();
                    while (reply_ != nullptr) {
                        r_lock_->wait();
                    }
                    reply_ = r;
                    r_lock_->unlock();
                    r_lock_->notify_all();
                } else if (k == MsgKind::Kill) {
                    // gets kill message from another node before server message
                    nodes_[m->sender_]->close_sock(); // close connection with that node
                    if (! teardown_) { // if teardown not already in progress
                        Kill* k = dynamic_cast<Kill*>(serv_->read_msg()); // wait for serv msg
                        check(k != nullptr, "Node: Invalid Kill message from server");
                        delete k;
                        teardown_node_();
                    }
                    delete m;
                } else check(false, "Node: Unexpected Message");
            }
        }

        // handles incoming messages from the sockets
        void handle_incoming_() {
            printf("Node %d: start listening for incoming messages\n", idx_);
            Socket** listen_to = new Socket*[num_nodes_];
            for (size_t i = 0; i < num_nodes_; ++i) listen_to[i] = nodes_[i];
            listen_to[idx_] = serv_;
            while (! teardown_) { // while teardown not initiated
                int active_idx = select_active(num_nodes_, listen_to);
                Message* m = listen_to[active_idx]->read_msg();
                check(m != nullptr, "Node: Received null message");
                handle_message_(m);
            }
            delete[] listen_to;
        }
        
        // runs the node
        // returns the index of this node
        int start() {
            // register with the server
            serv_register_();
            
            // receive directory
            Directory* dir = receive_directory_();
            // accept incoming connections
            accept_incoming_();
            // create outgoing connections
            create_outgoing_(dir);
            delete dir;

            // start listening thread
            listener_ = std::thread([this]{ this->handle_incoming_(); });
            
            return idx_;
        }
        
        // sends the given message
        // target node is the target of the message
        void send_to_node(Message* msg) {
            int idx = msg->target_;
            if (idx == SIDX) serv_->send_msg(msg);
            else {
                check(idx != idx_, "Node: Cannot send to self");
                check(idx >= 0 && (size_t)idx < num_nodes_, "Node: Index out of bounds");
                nodes_[idx]->send_msg(msg);
            }
        }

        // starts teardown for entire kvstore/network
        void teardown_all() {
            Kill* k = new Kill(idx_, SIDX);
            serv_->send_msg(k);
            delete k;
        }

        // tears down this node - private method
        // this method should only be called when kill received from server
        void teardown_node_() {
            teardown_ = true; // teardown has started
            handle_active_(); // handle active messages that have to be read
            serv_->close_sock(); // close server connection
            printf("Node %d: closed server connection\n", idx_);
            // recv kill and close all nodes before in list
            for (int i = 0; i < idx_; ++i) {
                if (nodes_[i]->is_closed()) continue;
                else {
                    Kill* k = dynamic_cast<Kill*>(nodes_[i]->read_msg());
                    nodes_[i]->close_sock();
                    delete k;
                }
            }
            // send kill to all nodes with greater idx in list
            Kill* k2 = new Kill(idx_, SIDX);
            for (size_t i = idx_+1; i < num_nodes_; ++i) {
                k2->update_target(i);
                nodes_[i]->send_msg(k2);
            }
            delete k2;
            // make sure all connections are closed
            for (size_t i = idx_+1; i < num_nodes_; ++i) nodes_[i]->wait_for_close();
            
            // clean up KVStore memory
            // has to be done here, when we know teardown is done
            kvs_->delete_all(); 
        }

        // puts the key and dataframe in the local kvstore if possible (if index matches)
        // else sends a Put message to the correct node
        void put(Key* k, DataFrame* v) {
            // if k->idx_ == this->idx_, put into nodekvs
            if (k->idx_ == idx_) kvs_->put(k, v);
            // else if k->idx_ !=, send put message to correct node
            else {
                Put* p = new Put(idx_, k, v);
                send_to_node(p);
                delete p;
            }
        }

        // waits for the reply for the get request with the given key
        DataFrame* wait_for_reply(Key* k) {
            r_lock_->lock();  
            while (reply_ == nullptr || !reply_->key_->equals(k)) {
                r_lock_->wait();
            }
            GetReply* r = reply_;
            reply_ = nullptr;
            r_lock_->unlock();
            r_lock_->notify_all();

            DataFrame* out = r->df_;
            delete r->key_;
            delete r;
            return out;
        }
        
        // gets the dataframe from the given key in the local kvstore (if index matches)
        // else sends Get message to correct node and waits for a reply
        DataFrame* get(Key* k) {
            if (k->idx_ == idx_) return kvs_->get(k);
            else {
                Get* g = new Get(idx_, k);
                send_to_node(g);
                delete g;
              
                return wait_for_reply(k); 
            }
        }

        // waits until the given key is in the kvstore to get the corresponding dataframe
        DataFrame* wait_and_get(Key* k) {
            if (k->idx_ == idx_) return kvs_->wait_and_get(k);
            else {
                WaitGet* w = new WaitGet(idx_, k);
                send_to_node(w);
                delete w;
            
                return wait_for_reply(k);
            }
        }
};
