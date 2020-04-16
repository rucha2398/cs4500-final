## Startup Detailed Explanation ##
1. Server starts up and starts listening for incoming connections on SIP:PORT
2. Nodes start up and connect to server
3. Nodes send Register messages to the server
    - This message is necessary to tell the server the IP of the node that is connecting
    - The IPs will go into the directory which will be used by the nodes when they need to
      directly connect to each other
    - The IPs also act as an identifier for the nodes so they can look up their index in the
      Directory by their own IP
    - Clarification: the server does not need to know the addresses of the nodes, only the nodes
      need to know each others addresses so they can open sockets with each other.
4. Server waits for all Register messages.
5. Server builds Directory message of IPs in the same order that the server accepted the incoming
   connections.
    - The index of a node is the based on the order which the server accepted the node connections.
6. Server broadcasts Directory messsage to all nodes.
7. Nodes figure out their own index from the Directory.
    - The index of that node's IP address in the directory is that node's index.
8. Node 0 waits for Connect message from server.
    - Node 0 doesn't have to send Open message since it does not accept incoming connections, 
      it only creates outgoing connections since it has the lowest index.
    - This wait is necessary to make sure all nodes > 0 are ready for incoming connections before
      Node 0 starts the connecting phase.
8. Nodes with indices > 0 open a listening socket for incoming connections.
9. Nodes with indices > 0 send Open message to server to indicate they're ready to accept.
    - The Open message fixes the issue where a node tries to connect to another node before the
      other node has started listening for incoming connections.
10. Server waits for Open messages from all nodes > 0.
11. Server sends Connect message to Node 0 to initiate connecting phase.
12. Connecting Phase:
    1. All nodes wait for incoming connections from nodes with a lower index.
        - ex. Node 3 waits for connections from Node 0, Node 1, and Node 2
        - Note: Node 0 is the lowest index, so it does not wait for any incoming connections
    2. All nodes wait for Greeting messages from all new connections.
        - This is necessary because nodes may not receive incoming connections in the same
          order as the Directory.
        - Greeting message includes the index of the sender node so that the receiving node
          will be able to correctly sort its connections list by index.
    3. All nodes connect to nodes with higher index.
        - Using address from Directory message for corresponding index.
        - Note: node n-1 does not create any outgoing connections since it is the highest index
13. Once all Greetings are sent, the node calls handle\_incoming() on a new thread which starts
    listening for and handling all incoming messages from other nodes/server during the process.

## Teardown Detailed Explanation ##
1. A Node initiates teardown by sending a Kill message to the server.
    - This way the user can decide which node and when the teardown will occur.
2. The server broadcasts the Kill message to all nodes in the network (including the node
   that initiated the teardown).
    - This is necessary so that all nodes know that teardown is starting and they should stop
      listening for new messages
3. Once a node receive Kill from server, start Disconnecting Phase:
    1. Node closes its socket connection to the server.
    2. Node waits for incoming Kill messages from all nodes with a lower index
    3. When Kill message is received, the receiving node closes the socket with the sender
        - This is necessary to fix the issue where a node closes a connection with another
          node before the other node has even initiated teardown (i.e. hasn't gotten server
          Kill message).
    4. Node sends Kill messages to all nodes with higher index
    5. Node waits for all nodes with higher index to close socket connection (wait\_for\_close())
        - Don't want to close connection or exit before Kill messages has been received,
          otherwise this will cause failures/exceptions with read
    6. Node Teardown Done.
4. Server waits for all nodes to close their server socket (wait\_for\_close()).
    - To make sure that all nodes received Kill message from server
5. Server Teardown Done.

## Disconnecting Phase - Node receives Kill from another Node first ##
1. Node receives Kill from another node before it receives server's Kill
2. Node closes connection with sender node.
3. Node waits for server's Kill message
4. Node performs normal Disconnecting Phase.
    - The only difference is that in step 2, if the socket was already closed, the node does
      not wait for a new Kill message (since it has already been read/consumed)
