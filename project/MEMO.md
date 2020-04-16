## Introduction ##

Our eau2 system is able to handle large amounts of data by distributing it over a network. First, it reads in the data from a SoR file and stores the data in a DataFrame. Then, the data is distributed over a network among nodes using a distributed Key-Value Storage system. The user can perform queries or calculations on the data which will have nodes communicate with each other to perform the necessary calculations. This provides a fast, distributed way of handling and analyzing data.


## Architecture ##

3 Layers
    1. Networking
        Our design is split into three layers of abstraction. The first layer is the networking layer. This layer handles low-level networking between nodes using a shared server. First, the server starts up using ./server <num_nodes>. Then each of the nodes register with the server using the given address. Then the nodes greet each other so they can create direct socket connections to each other. Then the nodes are able to send messages to each other such as Put, Get, GetReply, etc. We use a listener thread to handle incoming messages and spawn additional threads to for each call to wait_and_get(). The KVStore can interface with the node when it cannot find the right key in its local storage.  We abstract our serialization of messages, so it would be easy to add new message types to this implementation later on.
    2. Data
        The data layer has three sub-categories of source code. These include the dataframe, data adapter (sorer), and key-value storage. These three functions work together to handle parsing the data from a file, storing the data, and distributing it appropriately to the nodes.
    3. Application
       This layer will be used by customers. They can fill this layer with their own code that uses the KVStore interface. It contains an Application class that they can extend from to make their own applications.


## Implementation ##

Networking:
    Serialization:
        Our serialization and deserialization are local to whatever data is being serialized/deserialized. For example, the message serialization and deserialization methods are stored in their own class.There is also a serialize and a deserialize method on classes such as DataFrame and Key which have to be serialized to be sent over sockets. Our serialization uses readable serialization (i.e. all serialized messages are mostly readable/text-based). Our deserialization relies heavily on our own method in string.h called next_token() which can parse a string token by token.
    Client/Server:
        Our server starts up first and waits for incoming connections from all nodes. Once all the nodes register with the server, the server initiates the Connecting Phase. In this phase, all the nodes create direct connections with each other and exchange necessary information in a choreographed fasion. For the teardown, one node initiates the entire network teardown by sending a message to the server. The server then alerts all the nodes that teardown is starting and they all close their socket connections in an orderly manner. Lastly, the server waits for all the nodes to close down before exiting itself.
        ** For more details on the networking layer, look in the networking/protocol_description folder which contains step-by-step details on the setup, teardown, and usage of each message type. It also contains visual diagrams for how the network setup and teardown execute.

Data:
    Sorer:
        We chose to use The SegFault in Our Stars' implementation of a sorer. Most of the code is the same, however we removed the argument parsing. Also, instead of evaluating based on the command line arg passed and printing out a result, we converted the main function into a helper function that returns our own DataFrame type. We have a few tests in the sorer/test folder that make sure the sorer works with our DataFrame.
    DataFrame:
        We are using our own implementation based on the interfaces that we were given in Assignment 4. This code is fully functional since we have tested every aspect of it in the earlier assignments. We only added a few new methods to the Dataframe, namely from_array(), from_scalar, serialize, and deserialize. We also added a new file to the DataFrame code called split.h. This file contains a method for splitting up a DataFrame into smaller DataFrames by column. The split_by_row() function does not keep the rows consecutive, but does maintain the order (i.e. Column: 1 2 3 4 would be split into C1: 1 3, C2: 2 4).
    KV Store:
        We use 1 KVStore per node. The KVStore is able to communicate with other nodes by using the networking layer. The KVStore has reference to the local Node and can use that interface to send messages and wait for replies. The user code/application layer interfaces with this KVStore code to send and retrieve data from anywhere in the network. The user can initiate the teardown by calling a method on the KVStore (teardown()). The local data stored in the KVStore (i.e. the data whose key matches the application's index) are stored in a map from Key to DataFrame. For convenience, we also implemented a method called delete_all() on the KVStore which deletes all the keys and values stored on the local storage.

Application:
        Each application has its own KVStore that stores its local data (i.e. Data that is associated with keys that have that application's index).
    Demo:
        The demo runs by starting up the server and 3 nodes (using fork()). The first node is a producer, the second node is a counter, and the third node is the summarizer. The producer produces data while the other nodes wait on that key. Then the counter reads the data from the store. The summarizer verifies the data and also handles the teardown of the KVStore.
    Word Count:
        In the word_count application, we look for the number of distinct words in a file. We split up the file depending on the number of counters that the user gives as input. Node 0, the reader node handles reading the file and splitting up the words/formatting them for each counter node. Then the counter nodes count the number of distinct words in their data chunk from the reader. Lastly, the reducer combines all of the counts from the counter nodes into a final number of distinct words.
    Linus:
        Our Linus application has at least two nodes. Node 0 is the driver of the entire application and the other nodes perform calculations over the commits. The goal of the application is to calculate the number of users within DEG degrees of Linus Torvalds (DEG can be changed in the linus_node.cpp file, Linus class). First, Node 0 reads in the projects, users, and commits files. Then Node 0 creates a Set for the users and projects using the number of rows in the respective files. Next, Node 0 sends the count for the number of users and number of projects to all the other nodes in the system. They create their own Sets for projects/users which start empty. The node then reads in the commits file data. Node 0 splits up the resulting DataFrame into n DataFrames where n = num_nodes - 1. In other words, Node 0 divides the commits DataFrame into enough pieces, one for each node. Each node stores their commits locally. Then all of the nodes (including 0) start stepping for each degree. In each step, Node 0 first sends out the set of new users (which is initially only Linus). Then, each node calculates a Set of new projects based on the new users (i.e. new projects that one of the new users worked on) using their own commits. Node 0 then waits for the new projects from each node and merges the sets. Next, Node 0 sends a new projects Set to all of the nodes. The nodes then map through the commits and look for new users based on the new projects (i.e. new users that worked on the new projects). Node 0 then merges the sets of new users which marks the end of one step. After DEG steps, the program finishes by printing out the number of users.


## Use Cases ##
* Demo
    * Demo can be run by doing the following steps:
        * go to project/application/demo
        * run "make"
        * run "make run"
* Word Count
    * Word Count can be run by doing the following steps:
        * go to project/application/word_count
        * run "make"
        * run "make run"
* Linus
    * Note: The datasets (commits, projects, and users) are too large to upload, but they belong in the application/linus/datasets/big folder if you want to try and run linus
    * Linus can be run by doing the following steps:
        * go to project/application/linus
        * run "make"
        * run "make run" or "make time" to time

* All tests can be run using the Makefile in the project directory 
    * make : compiles all unit tests
    * make run : runs all tests
    * make run\_linus : runs linus from the project directory
    * make valgrind : runs valgrind on all of the tests and prints out results
    * make valgrind\_linus : run valgrind on linus from the project directory
    * make clean : cleans up all executables and files used by tests


## Open Questions ## 


## Status ##
Here is a list of our major tasks for the next few weeks. We will prioritize these items from the top down. 
    1. DONE: Pick a sorer implementation to use
    2. DONE: Integrate sorer w/the dataframe
    3. DONE: Key value store implementation (pm2)
    4. DONE: Write Application class for user to use (pm3)
    5. DONE: Finish networking (pm4)
        - Work on serialization (serializing dataframe and more specific messages to nodes)
        - Move to direct communication with nodes instead of through the server
        - Change nodes/Application class to use networking instead of threading
    6. DONE: cleanup code and tests
        - make DataFrame tests run again
        - make sure sorer tests run
    7. DONE: Read all messages before teardown
    8. DONE: Get linus working with small datasets
    9. DONE: Get linus working with large datasets (valgrind)
    11. DONE?: cleanup all code


Additional Notes:
 - Linus dataset off by one - could cause incorrect count since we discard rows that have data out of bounds
 - We know that word\_count does not correctly merge the data, but we did not have the chance to fully fix it. Right now, each node counts its own number of distinct words and then the total is the sum which does not take into accout the fact that words on different nodes could overlap.
