# AWS DynamoDB Clone

This is a project based in Java and run with Python scripts that attempts to emulate the Distributed Key-Value Store that AWS DynamoDB is based on.

The store can handle the capacity of reliably maintaining the storage of key-value pairs across 150 EC2 nodes. It will also be able to persist data in the case of failures of any nodes.

Some of the main features include:

- Load Balancing
- Reliable Key Transfer to successor nodes in case of failures
- Data Replication to maintain a 100% data recovery rate in case of hard crashes of any nodes
- Individual caching mechanisms to ensure rapid service

## Instructions to Run

Used Run Command: Server with 80 nodes: Create a directory 'output/', then run: python3 setup.py CPEN431_2024_PROJECT_G10-1.0-SNAPSHOT-jar-with-dependencies.jar server.txt output/ 5 30  
 Local server: java -Xmx64m -jar CPEN431_2024_PROJECT_G10-1.0-SNAPSHOT-jar-with-dependencies.jar localhost 43100 server_1.txt 5 30

## Brief Description: Distributed Key-value store based on consistent hashing.

### Java files:

- App: The main code to set up servers and initialize router and nodes
- Node: A class for the node object where the nodes are stored as a doubly linked list, storing both its predecessor and successor nodes
- Pair: A pair that stores the value and version together to store in the key-value store
- Router: The router holds a map of all the nodes and is where servers can get nodes from, add nodes, or invalidate dead nodes
- Server: Each of the servers is an object which has its own key-value store, and handles commands from the client
- SerializeUtils: Has the basic utility functions for the app such as serializing a message, making a message ID, and calculating checksum
- Epidemic: Set up threads to run epidemic protocol
- KVStore: An object for the key value store
- KVTransfer: Set up threads for key value store transferring with buckets

### Python files:

- setup: set up all servers listed in servers.txt
- destroy: destroy all servers that are running
