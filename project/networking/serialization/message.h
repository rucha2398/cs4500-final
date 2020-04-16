// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#pragma once

#include <stdio.h>
#include <netinet/in.h>
#include <string.h>
#include "../../data/kv_store/key.h"
#include "../../data/dataframe/dataframe.h"
#include "../../util/object.h"
#include "../../util/string.h"

const char EOM = '\n'; // means end of message (if not escaped)
// const char ESC = '\\'; escape character - declared in string.h
//const char* DLM = " ";  default deliminator between tokens - declared in string.h
const int IPLEN = 16; // equal to strlen("111.111.111.111")+1 (i.e. number of chars for an IP)
const int SIDX = -1; // index used to represent the server
const int UIDX = -2; // index used to represent a node that has not been registered

// enum representing different kinds of messages
enum class MsgKind { Register, Directory, Open, Connect, Greeting, Put, Get, WaitGet, GetReply, Text, Kill };

// this is a parent class for messages to be sent over a network
class Message : public Object {
    public:
        MsgKind kind_;  // the message kind
        int sender_; // index of the sender node 
        int target_; // index of the receiver node

        // constructor for a message
        // takes in the message kind, and two int to represent the sender and target nodes' indices
        Message(MsgKind kind, int sender, int target) : Object() {
            kind_ = kind;
            sender_ = sender;
            target_ = target;
        }
        
        // helper that copies the given src IP into the dest address
        // @pre the dest has at least IPLEN allocated bytes and that given src IP is valid
        static void copy_ip_(char* dest, const char* src) {
            size_t len = strlen(src);
            if (IPLEN - 1 < len) len = IPLEN - 1;
            memcpy(dest, src, sizeof(char) * (len));
            dest[len] = '\0';
        }

        // gets the sender of this message
        int get_sender() {
            return sender_;
        }

        // gets the target of this message
        int get_target() {
            return target_;
        }

        // updates the target of the message without changing any other data
        // used so multiple messages can be serialized and sent to different targets 
        // without copying data
        void update_target(int target) {
            target_ = target;
        }

        // converts a message kind into a string
        static char* kind_to_chars_(MsgKind k) {
            switch(k) {
                case MsgKind::Register:
                    return const_cast<char*>("Register");
                case MsgKind::Directory:
                    return const_cast<char*>("Directory");
                case MsgKind::Open:
                    return const_cast<char*>("Open");
                case MsgKind::Connect:
                    return const_cast<char*>("Connect");
                case MsgKind::Greeting:
                    return const_cast<char*>("Greeting");
                case MsgKind::Put:
                    return const_cast<char*>("Put");
                case MsgKind::Get:
                    return const_cast<char*>("Get");
                case MsgKind::WaitGet:
                    return const_cast<char*>("WaitGet");
                case MsgKind::GetReply:
                    return const_cast<char*>("GetReply");
                case MsgKind::Text:
                    return const_cast<char*>("Text");
                case MsgKind::Kill:
                    return const_cast<char*>("Kill");
                default:
                    check(false, "Invalid message type");
                    return nullptr;
            }
        }

        // wraps the given data with this message's header information
        // "<kind_> <sender_> <target_> {<data>}\n"
        char* wrap_with_header_(char* data) {
            StrBuff* sb = new StrBuff();

            sb->c(kind_to_chars_(kind_));

            sb->c(DLM);
            sb->c(sender_);
            sb->c(DLM);
            sb->c(target_);
            sb->c(" {");
            sb->c(data);
            sb->c("}");
            sb->c(EOM);

            char* tmp = sb->get();
            delete sb;
            return tmp;
        }

        // serializes this message
        virtual char* serialize() { 
            check(false, "Serialize called on parent Message class"); 
            return nullptr;
        }
        
        // deserializes the given character sequence into the correct message
        static Message* deserialize(char* m); 
};

// message sent by nodes to register with the server
// also send by nodes to register with other nodes
class Register : public Message {
    public:
        char node_[IPLEN];

        // creates a register message to register the given node
        Register(const char* node) : Message(MsgKind::Register, UIDX, SIDX) {
            copy_ip_((char*)node_, node);
        }

        // gets the IP of the node that is registering
        char* get_node() {
            return duplicate(node_);
        }

        // serializes this register message
        // "Register <UIDX> <SIDX> {<node_>}\n"
        char* serialize() {
            return wrap_with_header_(node_);
        }

        // deserializes the given string into a Register message
        static Register* deserialize(char* m) {
            char* rest = nullptr;
            char* kind = next_token(m, &rest, DLM, false);
            check(streq(kind, "Register"), "Invalid Register message");
            delete[] kind;

            delete[] next_token(rest, &rest, '{', ESC); // skips to the open bracket
            // gets the message's node
            char* node = next_token(rest, &rest, '}', false);

            Register* out = new Register(node);
            delete[] node;
            return out;
        }
};

// message sent by the server to tell all nodes which IPs are available to communicate with
class Directory : public Message {
    public:
        size_t size_; // represents the number of nodes in this directory
        char** addresses_;  // array of strings representing the nodes' IPs - stored internally
    
        // creates a directory message for the given node
        // copies the reference to the given address array so no extra copying
        // ordering of addresses is preserved from given list
        Directory(int target, const size_t size, char** addresses) : Message(MsgKind::Directory, SIDX, target) {
            size_ = size;
            // deep copy of addresses
            addresses_ = new char*[size];
            for (size_t i = 0; i < size; ++i) {
                addresses_[i] = duplicate(addresses[i]);
            }
        }

        // deletes addresses and array since they are copies of the given strings
        ~Directory() {
            for (size_t i = 0; i < size_; ++i) {
                delete[] addresses_[i];
            }
            delete[] addresses_;
        }

        // returns the number of nodes in this directory
        size_t size() { return size_; }

        // returns the address at the given index
        char* get(size_t idx) { return addresses_[idx]; }

        // returns true if this dictionary contains the given ip address
        bool contains(char* ip) {
            return index_of(ip) >= 0;
        }

        // returns the index of the given ip
        // returns -1 if this directory does not contain the given ip
        size_t index_of(char* ip) {
            for (size_t i = 0; i < size_; ++i) {
                if (strcmp(addresses_[i], ip) == 0) return i;
            }
            return -1;
        }

        // serializes a directory of node addresses in the following format:
        // "Directory <SIDX> <target_> {<nodes_> [<addresses_[0]> ... <addresses_[nodes_-1]>]}\n"
        char* serialize() {
            StrBuff* sb = new StrBuff();
            sb->c(size_);
            sb->c(" [");
            for (size_t i = 0; i < size_; ++i) {
                if (i != 0) sb->c(DLM);
                sb->c(addresses_[i]);
            }
            sb->c("]");
            char* data = sb->no_cpy_get();
            char* out = wrap_with_header_(data);
            delete[] data; // get() uses duplicate
            delete sb;
            return out;
        }

        // deserializes the given string into a directory message
        static Directory* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Directory"), "Invalid Directory message");
            delete[] tok;

            // skips the sender address
            delete[] next_token(rest, &rest, DLM, false);
            tok = next_token(rest, &rest, DLM, false);
            int target = atoi(tok);
            delete[] tok;

            // skips to inside the message
            delete[] next_token(rest, &rest, '{', false);
            tok = next_token(rest, &rest, DLM, false);
            size_t nodes = atoi(tok);
            delete[] tok;

            // skips to the start of the addresses
            delete[] next_token(rest, &rest, '[', false);
            char** addresses = new char*[nodes];
            for (size_t i = 0; i < nodes; ++i) {
                if (i < nodes - 1) addresses[i] = next_token(rest, &rest, DLM, false);
                else addresses[i] = next_token(rest, &rest, ']', false);
            }

            // TODO assert(strcmp(next_token(nullptr, DLM), "]") == 0);

            Directory* out = new Directory(target, nodes, addresses);
            for (size_t i = 0; i < nodes; ++i) delete[] addresses[i];
            delete[] addresses;
            return out;
        }
};

// message sent by a node to the server telling the server that it's open for incoming connections
class Open : public Message {
    public:
        Open(int sender) : Message(MsgKind::Open, sender, SIDX) { }

        // serializes this open message into the following format:
        // Open <sender> <SIDX> {}\n
        char* serialize() {
            return wrap_with_header_(const_cast<char*>(""));
        }

        // deserializes the given string into an Open message
        static Open* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Open"), "Invalid Open message");
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            int sender = atoi(tok);
            delete[] tok;

            return new Open(sender);
        }
};

// message sent by server to tell all nodes that all nodes are available to connect
class Connect : public Message {
    public: 
        Connect(int target) : Message(MsgKind::Connect, SIDX, target) { }

        // serializes this open message into the following format:
        // Connect <SIDX> <target> {}\n
        char* serialize() {
            return wrap_with_header_(const_cast<char*>(""));
        }

        // deserializes the given string into an Connect message
        static Connect* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Connect"), "Invalid Connect message");
            delete[] tok;

            // skip sender
            delete[] next_token(rest, &rest, DLM, false);
            tok = next_token(rest, &rest, DLM, false);
            int target = atoi(tok);
            delete[] tok;

            return new Connect(target);
        }
};

// message sent by a node to another node during socket setup
class Greeting : public Message {
    public:
        Greeting(size_t sender, size_t target) : Message(MsgKind::Greeting, sender, target) { }

        // serializes this greeting message into the following format:
        // "Greeting <sender_> <target_> {}\n"
        char* serialize() {
            return wrap_with_header_(const_cast<char*>(""));
        }

        // deserializes the given string into a greeting message
        static Greeting* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Greeting"), "Invalid Greeting message");
            delete[] tok;
            
            tok = next_token(rest, &rest, DLM, false);
            int sender = atoi(tok);
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            int target = atoi(tok);
            delete[] tok;
            return new Greeting(sender, target);
        }
};

// message sent from one node to another to put in a key
class Put : public Message {
    public:
        Key* key_;
        DataFrame* val_;
        
        Put(int sender, Key* key, DataFrame* val) : Message(MsgKind::Put, sender, key->idx_) {
            key_ = key;
            val_ = val;
        }

        // serializes this put message into the following format
        // dataframe: <col_types> <nrows> [[<data00> <data01> <data02>] [...]] 
        // key: <str> <idx>
        // Put <sender_> <target_> {<str> <idx>|<col_types> <nrows> [ ... ]}\n
        char* serialize() {
            StrBuff* sb = new StrBuff();
            char* tmp = key_->serialize();
            sb->c(tmp);
            delete[] tmp;

            sb->c('|');
            tmp = val_->serialize();
            sb->c(tmp);
            delete[] tmp;

            tmp = sb->no_cpy_get();
            char* out = wrap_with_header_(tmp); // don't want to use get() - extra dup
            delete[] tmp;
            delete sb;
            return out;
        }

        // deserializes a string into a put message
        // see serialize format above
        static Put* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Put"), "Invalid Put message");
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            int sender = atoi(tok);
            delete[] tok;

            // skip to inside of brackets
            delete[] next_token(rest, &rest, '{', false);
            tok = next_token(rest, &rest, '|', false); // key removes escapes when deserializing
            Key* k = Key::deserialize(tok);
            delete[] tok;

            tok = next_token(rest, &rest, '}', false); // dataframe removes escapes
            DataFrame* d = DataFrame::deserialize(tok);
            delete[] tok;

            return new Put(sender, k, d);
        }
};

// message sent from one node to another to put in a key
class Get : public Message {
    public:
        Key* key_;
        
        Get(int sender, Key* key) : Message(MsgKind::Get, sender, key->idx_) {
            key_ = key;
        }

        // serializes this get message into the following format:
        // Get <sender_> <target_> {<str> <idx>}\n
        char* serialize() {
            char* tmp = key_->serialize();
            char* out = wrap_with_header_(tmp);
            delete[] tmp;
            return out;
        }

        // deserializes the given string into a Get message
        static Get* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Get"), "Invalid Get message");
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            int sender = atoi(tok);
            delete[] tok;

            delete[] next_token(rest, &rest, '{', false);
            tok = next_token(rest, &rest, '}', true); // key handles escapes
            Key* k = Key::deserialize(tok);
            delete[] tok;

            return new Get(sender, k);
        }
};

// message sent from one node to another to put in a key
class WaitGet : public Message {
    public:
        Key* key_;
        
        WaitGet(int sender,  Key* key) : Message(MsgKind::WaitGet, sender, key->idx_) {
            key_ = key;
        }

        // serializes this waitget message into the following format:
        // WaitGet <sender_> <target_> {<str> <idx>}\n
        char* serialize() {
            char* tmp = key_->serialize();
            char* out = wrap_with_header_(tmp);
            delete[] tmp;
            return out;
        }

        // deserializes the given string into a WaitGet message
        static WaitGet* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "WaitGet"), "Invalid WaitGet message");
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            int sender = atoi(tok);
            delete[] tok;

            delete[] next_token(rest, &rest, '{', false);
            tok = next_token(rest, &rest, '}', true); // key handles escapes
            Key* k = Key::deserialize(tok);
            delete[] tok;

            return new WaitGet(sender, k);
        }
};

// message sent from one node to another in response to a get request
class GetReply : public Message {
    public:
        Key* key_; // key from the get request
        DataFrame* df_; // corresponding dataframe
        
        // creates a GetReply message with the given target, key, and dataframe
        GetReply(int target, Key* key, DataFrame* df) : Message(MsgKind::GetReply, key->idx_, target) {
            key_ = key;
            df_ = df;
        }

        // serializes this GetReply message into the following format:
        // GetReply <sender_> <target_> {<str> <idx>|<col_types> <nrows> [...]}\n
        char* serialize() {
            StrBuff* sb = new StrBuff();
            char* tmp = key_->serialize();
            sb->c(tmp);
            delete[] tmp;

            sb->c('|');
            tmp = df_->serialize();
            sb->c(tmp);
            delete[] tmp;

            tmp = sb->no_cpy_get();
            char* out = wrap_with_header_(tmp); // don't want to use get(), extra dup
            delete[] tmp;
            delete sb;
            return out;
        }

        static GetReply* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "GetReply"), "Invalid GetReply message");
            delete[] tok;

            // skip the sender
            delete[] next_token(rest, &rest, DLM, false);

            tok = next_token(rest, &rest, DLM, false);
            int target = atoi(tok);
            delete[] tok;

            // skip to inside of brackets
            delete[] next_token(rest, &rest, '{', false);
            tok = next_token(rest, &rest, '|', false); // key removes escapes when deserializing
            Key* k = Key::deserialize(tok);
            delete[] tok;

            tok = next_token(rest, &rest, '}', false); // dataframe removes escapes
            DataFrame* d = DataFrame::deserialize(tok);
            delete[] tok;

            return new GetReply(target, k, d);
        }
};


// message sent by a node to another node
// can be used to wrap another serializable class
// ex. pass serialized Class to Text constructor to serialize
// deserialize into Text message, then deserialize text into Class type
class Text : public Message {
    public: 
        const char* text_; // text of the message ending in null char - internal copy

        // creates a text message with the given sender, target, and message
        // all args are external - reference to text is copied
        // @pre given text ends in a null char
        Text(int sender, int target, const char* text) : Message(MsgKind::Text, sender, target) {
            text_ = duplicate(text);
        }

        // deconstructor for text since it is a copy of the given text
        ~Text() {
            delete[] text_; // delete[]  because duplicate
        }

        // serializes a text message from one node to another in the following format:
        // "Text <sender_> <target_> {<excaped t->text_>}\n"
        char* serialize() {
            char to_esc[] = {EOM, ESC, '}', '\0'};
            char* escaped = add_escapes(text_, to_esc);
            char* out = wrap_with_header_(escaped); // does not free given data
            delete[] escaped; // because add_escapes uses StrBuff::get() which uses duplicate()
            return out;
        }

        // deserializes the given string into a text message
        static Text* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Text"), "Invalid Text message");
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            int sender = atoi(tok);
            delete[] tok;

            tok = next_token(rest, &rest, DLM, false);
            int target = atoi(tok);
            delete[] tok;

            delete[] next_token(rest, &rest, '{', ESC); // skips to the open bracket
            // gets the message's node
            char* text = next_token(rest, &rest, '}', true);

            Text* out = new Text(sender, target, text);
            delete[] text;
            return out;

        }
};

// message sent by the server to tell nodes to shutdown
class Kill : public Message {
    public:
        // creates a kill message to be sent to the given node
        Kill(int sender, int target) : Message(MsgKind::Kill, sender, target) {}

        // gets the IP of the node that is supposed to receive the kill message
        int get_node() {
            return target_;
        }

        // serializes a kill message from the server to other nodes in the following format:
        // "Kill <SIDX> <target_> {}\n"
        char* serialize() {
            return wrap_with_header_(const_cast<char*>(""));
        }

        // deserializes a string into a kill message
        static Kill* deserialize(char* m) {
            char* rest = nullptr;
            char* tok = next_token(m, &rest, DLM, false);
            check(streq(tok, "Kill"), "Invalid Kill message");
            delete[] tok;
            
            // get the sender
            tok = next_token(rest, &rest, DLM, false);
            int sender = atoi(tok);
            delete[] tok;
            // gets the message's target and uses it as the node
            tok = next_token(rest, &rest, DLM, false);
            int node = atoi(tok);
            delete[] tok;

            return new Kill(sender, node);
        }
};


// deserializes the given character sequence into the correct message
Message* Message::deserialize(char* m) {
    // TODO switch away from next_token to avoid duplication
    char* rest = nullptr;
    char* kind = next_token(m, &rest, DLM, ESC);

    Message* out = nullptr;

    if (streq(kind, "Register")) out = Register::deserialize(m);
    else if (streq(kind, "Directory")) out = Directory::deserialize(m);
    else if (streq(kind, "Open")) out = Open::deserialize(m);
    else if (streq(kind, "Connect")) out = Connect::deserialize(m);
    else if (streq(kind, "Greeting")) out = Greeting::deserialize(m);
    else if (streq(kind, "Put")) out = Put::deserialize(m);
    else if (streq(kind, "Get")) out = Get::deserialize(m);
    else if (streq(kind, "WaitGet")) out = WaitGet::deserialize(m);
    else if (streq(kind, "GetReply")) out = GetReply::deserialize(m);
    else if (streq(kind, "Text")) out = Text::deserialize(m);
    else if (streq(kind, "Kill")) out = Kill::deserialize(m);
    else check(false, "Invalid Message kind");

    delete[] kind;
    return out;
}
