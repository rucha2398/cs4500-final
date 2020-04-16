// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#pragma once

#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include "./serialization/message.h"
#include "../util/string.h"
#define PORT 8080

const bool DEBUG = false; // true if you want to print debug messages
const char* SIP = "127.1.0.1"; // server IP
const int SIZE = 1024; // buffer size

// class that holds one socket and abstracts over it
class Socket : public Object {
    public:
        int sock_;
        bool closed_; // flag that determines if this socket is closed
        bool active_; // flag that marks if this socket is active (has a message to read)
        char read_[SIZE]; // string buffer that contains partially read messages

        // wraps the given socket fd
        Socket(int sock) : Object() {
            sock_ = sock;
            closed_ = false;
            active_ = false;
            read_[0] = '\0';
        }

        // deconstructor
        ~Socket() { 
            close_sock();
        }

        // sets up a socket using the given destination address from the constructor
        static Socket* connect_to_addr(const char* addr, int port) {
            struct sockaddr_in dest_addr;
            int ret;
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            check(sock >= 0, "Socket creation error");

            dest_addr.sin_family = AF_INET;
            dest_addr.sin_port = htons(port);

            // Convert IPv4 and IPv6 addresses from text to binary form
            ret = inet_pton(AF_INET, addr, &dest_addr.sin_addr);
            check(ret > 0, "Invalid address");

            // try to connect to the destination
            ret = connect(sock, (struct sockaddr *)&dest_addr, sizeof(dest_addr));
            if (ret < 0) {
                printf("Connection failed: addr = %s %d\n", addr, port);
                exit(-1);
            }

            return new Socket(sock);
        }

        // accepts n connections to the given address
        // send the given Open message to the given Socket once the listening socket is open
        // returns a list of sockets of size n
        // returns nullptr if n=0
        static Socket** accept_n(const char* in_addr, int port, size_t n, Socket* serv, Open* omsg) {
            if (n == 0) return nullptr;
            int fd;
            struct sockaddr_in address;
            int opt = 1;
            int ret;
            
            // creating socket file descriptor
            fd = socket(AF_INET, SOCK_STREAM, 0);
            check(fd != 0, "Socket failed");

            // forcefully attaching socket to the PORT (8080)
            ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));
            check(ret == 0, "setsockopt failed");

            address.sin_family = AF_INET;
            address.sin_port = htons(port);
            ret = inet_pton(AF_INET, in_addr, &address.sin_addr);
            check(ret > 0, "Invalid address");

            int addrlen = sizeof(address);

            // Forcefully attaching socket to the PORT (8080)
            ret = bind(fd, (struct sockaddr *)&address, sizeof(address));
            check(ret == 0, "Bind failed");
            
            ret = listen(fd, 3);
            check(ret == 0, "Listen failed");
            Socket** out = new Socket*[n];
            int new_sock = -1;
            if (serv != nullptr && omsg != nullptr) serv->send_msg(omsg);
            for (size_t i = 0; i < n; ++i) {
                // incoming connection lasts for certain amount of time
                new_sock = accept(fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
                check(new_sock >= 0, "Accept failed");
                out[i] = new Socket(new_sock);
            }

            close(fd); // closes listening socket once done

            return out;
        }

        // accepts n connections to the given address
        // returns a list of sockets of size n
        // returns nullptr if n=0
        static Socket** accept_n(const char* in_addr, int port, size_t n) {
            return accept_n(in_addr, port, n, nullptr, nullptr);
        }
        
        // send message over this socket
        void send_msg(Message* msg) {
            check(!closed_, "Cannot send message on closed socket");
            char* sm = msg->serialize();
            if (DEBUG) printf("Sent: %s", sm);
            check(send(sock_, sm, strlen(sm), 0) >= 0, "Send failed");
            delete[] sm;
        }
        
        // reads a message from this socket
        // returns nullptr if it couldn't read a full message
        Message* read_msg() {
            check(!closed_, "Cannot receive message from closed socket");
            // we're using a string buffer to handle a variable size array
            // user read_ instead of sb
            StrBuff* sb = new StrBuff();

            bool msg_done = false;
            while (! msg_done) {
                int idx = index_of(read_, EOM, true); // ignores escaped EOMs
                if (idx < 0) {
                    sb->c(read_);
                    memset(read_, '\0', SIZE * sizeof(char));
                    int chars_read = read(sock_, read_, SIZE - 1);
                    check(chars_read >= 0, "Invalid Message read");
                } else {
                    // store first part up to EOM in sb
                    read_[idx] = '\0'; // overwrite EOM with '\0'
                    sb->c(read_); // concat read_ up to idx
                    sb->c(EOM); // concat on overwritten EOM
                    // copy rest to start of read_
                    memcpy(read_, &(read_[idx+1]), strlen(&(read_[idx+1])) + 1);
                    msg_done = true;
                }
            }

            char* tmp = sb->no_cpy_get();
            if (DEBUG) printf("Recv: %s", tmp);
            Message* out = Message::deserialize(tmp);

            if (read_[0] == '\0') active_ = false;
            else active_ = true;

            delete sb;
            delete[] tmp;
            return out;
        }

        // closes this socket and sets the closed flag to true
        // socket can only be closed once
        void close_sock() {
            if (! closed_) close(sock_);
            closed_ = true;
        }

        // checks if this socket is closed
        // https://stackoverflow.com/questions/12402549/check-if-socket-is-connected-or-not
        bool is_closed() {
            if (closed_) return true;
            char buf[1];
            int ret = recv(sock_, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
            if ((ret == -1 && !(errno == EAGAIN || errno == EWOULDBLOCK)) || ret == 0) {
                closed_ = true;
                return true;
            } else return false;
        }

        // waits for this socket to be closed
        void wait_for_close() {
            while (! is_closed()) sleep(0.2);
        }
};


// returns the index of one of the socket that is currently active
// sets the active_ flag on all the sockets that are active
int select_active(int size, Socket** socks) {
    for (int i = 0; i < size; ++i) {
        if (socks[i]->active_) return i;
    }
    
    // make fd set
    fd_set set;
    FD_ZERO(&set);
    int nfds = -1;
    for (int i = 0; i < size; ++i) {
        int cur = socks[i]->sock_;
        if (cur > nfds) nfds = cur;
        FD_SET(cur, &set);
    }
    ++nfds;
    // select waits for socket to be active
    check(select(nfds, &set, nullptr, nullptr, nullptr) > 0, "Select failed");
    // check which socket is active and return its index
    int out = -1;
    for (int i = 0; i < size; ++i) {
        int cur_sock = socks[i]->sock_;
        if (FD_ISSET(cur_sock, &set)) {
            if (out == -1) out = i;
            socks[i]->active_ = true; // socket is active
        }
    }
    check(out >= 0, "Couldn't find active socket");
    return out;
}
