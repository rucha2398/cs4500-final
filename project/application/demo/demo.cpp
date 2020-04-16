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
    check(argc == 1, "Usage: ./demo");

    char** args = new char*[3];
    int serv_pid = fork();
    if (serv_pid == 0) {
        args[0] = const_cast<char*>("../../networking/server");
        args[1] = const_cast<char*>("3"); // producer, consumer, summarizer
        args[2] = nullptr;
        execvp(args[0], args);
        check(false, "Server exec failed");
    }

    sleep(1);
    // # args = 2: ./demo_node <ip>
    int pids[3];
    args[0] = const_cast<char*>("./demo_node");
    for (int i = 0; i < 3; ++i) {
        // build up ips - IP = 127.0.0.<idx>
        char ip[IPLEN];
        int len = strlen("127.0.0.");
        memcpy(ip, "127.0.0.", len);
        char idx[4];
        sprintf(idx, "%d", i+1);
        memcpy(&(ip[len]), idx, strlen(idx) + 1);
        args[1] = ip;
        args[2] = nullptr;

        // spawn child processes for each node
        pids[i] = fork();
        if (pids[i] == 0) {
            execvp(args[0], args);
            check(false, "Node exec failed");
        }
    }

    delete[] args;
    int status;
    waitpid(serv_pid, &status, 0);
    for (int i = 0; i < 3; ++i) {
        waitpid(pids[i], &status, 0);
    }
}
