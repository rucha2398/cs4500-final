// Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

#include "../../../util/object.h"
#include "../../../util/string.h"
#include "../../../util/helper.h"
#include "../message.h"
#include <stdio.h>
#include <string.h>

// tests serialization and deserialization of Register message
void testReg() {
    char* node1 = const_cast<char*>("127.0.0.1");
    Register* r = new Register(node1);
    
    char* rs = r->serialize();
    printf("%s", rs);
    check(streq(rs, "Register -2 -1 {127.0.0.1}\n"), "Register serialization failed");
    
    Register* rd = dynamic_cast<Register*>(Message::deserialize(rs));
    check(rd != nullptr, "Cast failed");
    check(r->kind_ == MsgKind::Register, "Incorrect kind");
    char* tmp = r->get_node();
    char* tmp2 = rd->get_node();
    check(streq(tmp, tmp2), "Incorrect node");

    delete r;
    delete[] rs;
    delete rd;
    delete[] tmp;
    delete[] tmp2;

    puts("Test Register passed");
}

// tests serialization and deserialization of Directory message
void testDir() {
    int target = 1;
    size_t size = 3;
    char** addrs = new char*[size];// {"127.0.0.1", "127.0.0.2", "127.0.0.3"};
    addrs[0] = const_cast<char*>("127.0.0.1");
    addrs[1] = const_cast<char*>("127.0.0.2");
    addrs[2] = const_cast<char*>("127.0.0.3");
    Directory* d = new Directory(target, size, addrs);
    delete[] addrs;

    char* ds = d->serialize();
    printf("%s", ds);
    check(streq(ds, "Directory -1 1 {3 [127.0.0.1 127.0.0.2 127.0.0.3]}\n"), 
            "Directory serialization failed");
    
    Directory* dd = dynamic_cast<Directory*>(Message::deserialize(ds));
    check(dd != nullptr, "Cast failed");
    check(dd->kind_ == MsgKind::Directory, "Incorrect kind");
    check(d->sender_ == dd->sender_, "Incorrect sender");
    check(d->target_ == dd->target_, "Incorrect target");
    check(d->size_ == dd->size_, "Incorrect size");
    for (int i = 0; i < size; ++i) {
        printf("dd->a[%d] = %s\n", i, dd->addresses_[i]);
        check(streq(addrs[i], dd->addresses_[i]), "Incorrect address");
    }

    delete d;
    delete[] ds;
    delete dd;

    puts("Test Directory passed");
}

// tests serialization and deserialization of Open message
void testOpen() {
    int node1 = 1;
    Open* o = new Open(node1);

    char* os = o->serialize();
    printf("%s", os);
    check(streq(os, "Open 1 -1 {}\n"), "Open serialization failed");

    Open* od = dynamic_cast<Open*>(Message::deserialize(os));
    check(od != nullptr, "Cast failed");
    check(o->kind_ == MsgKind::Open, "Incorrect kind");
    check(o->sender_ == od->sender_, "Mismatched nodes");

    delete o;
    delete[] os;
    delete od;

    puts("Test Open passed");
}

// tests serialization and deserialization of Connect message
void testConnect() {
    int node1 = 1;
    Connect* c = new Connect(node1);

    char* cs = c->serialize();
    printf("%s", cs);
    check(streq(cs, "Connect -1 1 {}\n"), "Connect serialization failed");

    Connect* cd = dynamic_cast<Connect*>(Message::deserialize(cs));
    check(cd != nullptr, "Cast failed");
    check(c->kind_ == MsgKind::Connect, "Incorrect kind");
    check(c->target_ == cd->target_, "Mismatched nodes");

    delete c;
    delete[] cs;
    delete cd;

    puts("Test Connect passed");
}

// tests serialization and deserialization of Greeting message
void testGreet() {
    Greeting* g = new Greeting(1, 2);
    
    char* gs = g->serialize();
    printf("%s", gs);
    check(streq(gs, "Greeting 1 2 {}\n"), "Incorrect Greeting serializaton");

    Greeting* gd = Greeting::deserialize(gs);
    check(gd->kind_ == MsgKind::Greeting, "Kind not Greeting");
    check(g->sender_ == gd->sender_, "Incorrect sender");
    check(g->target_ == gd->target_, "Incorrect target");
    
    delete g;
    delete[] gs;
    delete gd;

    puts("Test Greeting passed");
}

// tests serialization and deserialization of Put message
void testPut() {
    Key* k = new Key("wier|]d }chars\\\n", 2);
    Schema* s = new Schema("BI");
    s->add_row();
    s->add_row();
    s->add_row();
    DataFrame* df = new DataFrame(*s);
    bool b = 1;
    df->set(0, 0, b);
    b = 0;
    df->set(0, 1, b);
    b = 1;
    df->set(0, 2, b);
    df->set(1, 0, 12);
    df->set(1, 1, -15);
    df->set(1, 2, 256);

    Put* p = new Put(1, k, df);
    char* ps = p->serialize();
    printf("%s", ps);
    check(streq(ps, "Put 1 2 {wier\\|\\]d\\ \\}chars\\\\\\\n 2|BI 3 [[1 0 1] [12 -15 256]]}\n"), 
            "Incorrect Put serialization");

    Put* pd = Put::deserialize(ps);
    check(pd->kind_ == MsgKind::Put, "Kind not Put");
    check(pd->sender_ == p->sender_, "Incorrect sender");
    check(pd->target_ == p->target_, "Incorrect target");
    check(pd->key_->equals(p->key_), "Incorrect key");
    check(pd->val_->get_bool(0, 2) == 1, "Mismatched data");
    check(pd->val_->get_int(1, 1) == -15, "Mismatched data");

    delete k;
    delete s;
    delete df;
    delete p;
    delete[] ps;
    delete pd->key_;
    delete pd->val_;
    delete pd;

    puts("Test Put passed");
}

// tests serialization and deserializatoin of Get message
void testGet() {
    Key* k = new Key("Key1", 1);
    int sender = 1;
    Get* g = new Get(sender, k);

    char* gs = g->serialize();
    printf("%s", gs);
    check(streq(gs, "Get 1 1 {Key1 1}\n"), "Get serialization failed");

    Get* gd = dynamic_cast<Get*>(Message::deserialize(gs));
    check(gd != nullptr, "Cast failed");
    check(g->kind_ == MsgKind::Get, "Incorrect kind");
    check(g->get_sender() == gd->get_sender(), "Incorrect sender");
    check(g->get_target() == gd->get_target(), "Incorrect target");
    check(g->key_->equals(gd->key_), "Mismatched keys");

    delete k;
    delete g;
    delete[] gs;
    delete gd->key_;
    delete gd;

    puts("Test Get passed");
}

// tests serialization and deserializatoin of WaitGet message
void testWaitGet() {
    Key* k = new Key("Key1", 1);
    int sender = 1;
    WaitGet* wg = new WaitGet(sender, k);

    char* wgs = wg->serialize();
    printf("%s", wgs);
    check(streq(wgs, "WaitGet 1 1 {Key1 1}\n"), "Wait Get serialization failed");

    WaitGet* wgd = dynamic_cast<WaitGet*>(Message::deserialize(wgs));
    check(wgd != nullptr, "Cast failed");
    check(wg->kind_ == MsgKind::WaitGet, "Incorrect kind");
    check(wg->get_sender() == wgd->get_sender(), "Incorrect sender");
    check(wg->get_target() == wgd->get_target(), "Incorrect target");
    check(wg->key_->equals(wgd->key_), "Mismatched keys");

    delete k;
    delete wg;
    delete[] wgs;
    delete wgd->key_;
    delete wgd;

    puts("Test WaitGet passed");
}

// tests serialization and deserializatoin of GetReply message
void testGetReply() {
    Key* k = new Key("wier|]d }chars\\\n", 2);
    Schema* s = new Schema("BI");
    s->add_row();
    s->add_row();
    s->add_row();
    DataFrame* df = new DataFrame(*s);
    bool b = 1;
    df->set(0, 0, b);
    b = 0;
    df->set(0, 1, b);
    b = 1;
    df->set(0, 2, b);
    df->set(1, 0, 12);
    df->set(1, 1, -15);
    df->set(1, 2, 256);

    GetReply* r = new GetReply(1, k, df);
    char* rs = r->serialize();
    printf("%s", rs);
    check(streq(rs, "GetReply 2 1 {wier\\|\\]d\\ \\}chars\\\\\\\n 2|BI 3 [[1 0 1] [12 -15 256]]}\n"),
            "Incorrect GetReply serialization");

    GetReply* rd = GetReply::deserialize(rs);
    check(rd->kind_ == MsgKind::GetReply, "Kind not GetReply");
    check(rd->sender_ == r->sender_, "Incorrect sender");
    check(rd->target_ == r->target_, "Incorrect target");
    check(rd->key_->equals(r->key_), "Incorrect key");
    check(rd->df_->get_bool(0, 2) == 1, "Mismatched data");
    check(rd->df_->get_int(1, 1) == -15, "Mismatched data");

    delete k;
    delete s;
    delete df;
    delete r;
    delete[] rs;
    delete rd->key_;
    delete rd->df_;
    delete rd;

    puts("Test GetReply passed");
}

// tests serialization and deserialization of Text message
void testText() {
    int sender = 1;
    int target = 2;
    char* text = const_cast<char*>("hello world }} add some \n weird \\ chars");
    Text* t = new Text(sender, target, text);

    char* ts = t->serialize();
    printf("%s", ts);
    check(streq(ts, "Text 1 2 {hello world \\}\\} add some \\\n weird \\\\ chars}\n"), 
            "Text serialization failed");

    Text* td = dynamic_cast<Text*>(Message::deserialize(ts));
    check(td != nullptr, "Cast failed");
    check(td->kind_ == MsgKind::Text, "Incorrect Kind");
    check(t->sender_ == td->sender_, "Incorrect sender");
    check(t->target_ == td->target_, "Incorrect target");
    check(streq(t->text_, td->text_), "Mismatched text");

    delete t;
    delete[] ts;
    delete td;

    puts("Test Text passed");
}

// tests serialization and deserialization of Kill message
void testKill() {
    int node1 = 1;
    Kill* k = new Kill(SIDX, node1);

    char* ks = k->serialize();
    printf("%s", ks);
    check(streq(ks, "Kill -1 1 {}\n"), "Kill serialization failed");

    Kill* kd = dynamic_cast<Kill*>(Message::deserialize(ks));
    check(kd != nullptr, "Cast failed");
    check(k->kind_ == MsgKind::Kill, "Incorrect kind");
    check(k->get_node() == kd->get_node(), "Mismatched nodes");

    delete k;
    delete[] ks;
    delete kd;

    puts("Test Kill passed");
}


int main() {
    testReg();
    testDir();
    testOpen();
    testConnect();
    testGreet();
    testPut();
    testGet();
    testWaitGet();
    testGetReply();
    testText();
    testKill();
    
    puts("All tests passed");

    return 0;
}
