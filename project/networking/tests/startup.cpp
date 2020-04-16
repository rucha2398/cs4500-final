#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include "../../util/helper.h"
#include "../serialization/message.h"

// starts up the server and n nodes
// n passed in command line
// used test server and nodes and as template for other tests
int main(int argc, char** argv) {
    check(argc == 2, "Usage: ./startup <num_nodes>");
    int num_nodes = atoi(argv[1]);

    char** args = new char*[3];
    int serv_pid = fork();
    if (serv_pid == 0) {
        args[0] = const_cast<char*>("../server");
        args[1] = duplicate(argv[1]);
        args[2] = nullptr;
        execvp(args[0], args);
        check(false, "Server exec failed");
    } else printf("serv_pid = %d\n", serv_pid);
    
    sleep(1); // sleep to make sure server starts up before nodes

    // # args = 2: ./node <ip>
    int* pids = new int[num_nodes];
    args[0] = const_cast<char*>("./node");
    for (int i = 0; i < num_nodes; ++i) {
        char ip[IPLEN];
        int len = strlen("127.0.0.");
        memcpy(ip, "127.0.0.", len);
        char idx[4];
        sprintf(idx, "%d", i+1);
        memcpy(&(ip[len]), idx, strlen(idx) + 1);
        puts(ip);
        args[1] = ip;
        args[2] = nullptr;
        
        pids[i] = fork();
        if (pids[i] == 0) {
            execvp("./node", args);
            check(false, "Node exec failed");
        } else printf("pids[%d] = %d\n", i, pids[i]);
    }
    
    delete[] args;
    int status;
    waitpid(serv_pid, &status, 0);
    for (int i = 0; i < num_nodes; ++i) {
        waitpid(pids[i], &status, 0);
    }
    delete[] pids;
}
