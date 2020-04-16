#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include "../../util/helper.h"
#include "../../util/string.h"
#include "../../networking/serialization/message.h" // for IPLEN

// starts up the server and n nodes
// n passed in command line
// used test server and nodes and as template for other tests
int main(int argc, char** argv) {
    check(argc == 2, "Usage: ./linus <num_nodes>");

    int num_nodes = atoi(argv[1]);

    int serv_pid = fork();
    if (serv_pid == 0) {
        char** serv_args = new char*[3];
        serv_args[0] = const_cast<char*>("../../networking/server");
        serv_args[1] = new char[3]; // up to 3 digits for number of counters allowed
        serv_args[2] = nullptr;
        sprintf(serv_args[1], "%d", num_nodes); // counters + reader and reducer
        execvp(serv_args[0], serv_args);
        check(false, "Server exec failed");
    }

    sleep(1); // make sure server is started up
    int* pids = new int[num_nodes];
    for (int i = 0; i < num_nodes; ++i) {
        // spawn child processes for each node
        pids[i] = fork();
        if (pids[i] == 0) {
            // # args = 3: ./linus_node <ip> <num_nodes>
            char** node_args = new char*[4];
            node_args[0] = const_cast<char*>("./linus_node");
            // build up ips - IP = 127.0.0.<idx+1>
            StrBuff* sb = new StrBuff();
            sb->c("127.0.0.");
            sb->c(i+1);
            node_args[1] = sb->get();
            delete sb;

            node_args[2] = argv[1]; // num_nodes
            node_args[3] = nullptr;

            execvp(node_args[0], node_args);
            check(false, "Node exec failed");
        }
    }

    int status;
    waitpid(serv_pid, &status, 0);
    for (int i = 0; i < 3; ++i) {
        waitpid(pids[i], &status, 0);
    }
    delete[] pids;
}
