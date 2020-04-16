#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include "../../util/helper.h"
#include "../../networking/serialization/message.h" // for IPLEN

// starts up the server and n nodes
// n passed in command line
// used test server and nodes and as template for other tests
int main(int argc, char** argv) {
    check(argc == 3, "Usage: ./word_count <filename> <num_counters>");
    
    const int n_counters = atoi(argv[2]);
    const int num_nodes = n_counters + 2;

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

    sleep(1);
    int* pids = new int[num_nodes];
    for (int i = 0; i < num_nodes; ++i) {
        // spawn child processes for each node
        pids[i] = fork();
        if (pids[i] == 0) {
            // # args = 4: ./wc_node <ip> <filename> <num_counters>
            char** node_args = new char*[5]; 
            node_args[0] = const_cast<char*>("./wc_node");
            // build up ips - IP = 127.0.0.<idx+1>
            char* ip = new char[IPLEN];
            int len = strlen("127.0.0.");
            memcpy(ip, "127.0.0.", len);
            char idx[4];
            sprintf(idx, "%d", i+1);
            memcpy(&(ip[len]), idx, strlen(idx) + 1);
            node_args[1] = ip;
            
            node_args[2] = argv[1]; // filename
            node_args[3] = argv[2]; // n_counters
            node_args[4] = nullptr;
            
            execvp(node_args[0], node_args);
            perror("");
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
