memkv

Memkv is a simple in memory key value store that supports the following features:
* Get or Set a single key
* Get multiple values for multiple keys
* Set multiple keys to independent values
* Delete a key
* Obtain metrics data about the server

The in-memory store can support multiple clients in parallel and provides a consistent view of the data when a key is simultaneously accessed by multiple clients.


Prerequisites:

* You must a version of python 3.10+ installed.  Ideally this is done using a virtual environment.
* You must have pip version N.N installed into the virtual environment or global python 3 distribution
* Protobuf support should be installed.  Instructions are available here: https://developers.google.com/protocol-buffers.  Follow the download and install instructions under "How do I start?"


Installation:

Unpack the memkv.zip file into a folder
1. cd to the memkv folder
2. Switch to a python 3.10+ virtual environment if necessary
3. run `pip install -r requirements.txt`
4. run `protoc -I=. --python-out=${HOME}/{path to this folder}/memkv ${HOME}/{path to this folder}/memkv.proto`


Usage:


Contribute:

Code layout:
* src/   # The directory where the source lives
* tests/ # The folder that contains the tests
* README.md # This file


High Level Design
At a high level this is a simple TCP/IP server that listens on a port, accepts clients, parses the request from the client, wraps it in a message and then posts it to a queue.  The queue is shared with a pool of worker processes.  Each worker process consumes a message at a time off of the queue.  The request is unwrapped from the message and processed by the worker.  

The worker processes the request by reading or writing from a shared Dictionary that is guarded by a Read/Write lock.  The lock allows reads until a write is detected.  Once the write is detected any new workers will need to wait for the write to complete.  The write cannot happen until all previous reads are complete.

Wire Protocol
The wire protocol consists of Google Protocol buffers for efficiency and simplicity.  Every request to the server is encapsulated by a Message object.  It contains either a `get`, `set`, or `delete` command object.  The `get` and `set` commands support getting or setting multiple keys.

The protocol buffer definitions live in the base of the repository in memkv.proto


Detailed Server Design

The server consists of a main process and a pool of worker processes.  The main processes accepts connections from clients and then reads the data asynchronously and wraps it with a MessageWrapper (which holds the header and the serialized command).  Each message is then added to a queue which is consumed by the worker processes.  It uses asyncio to allow maximum io throughput on the main process.

The Main Process
The main process is responsible for:
* Accepting client sockets
* Reading requests off of those sockets
* Pushing the requests onto a shared queue
* Holding a reference to the hash table.
* Manages a pool of processes that consume the requests on the shared queue

As you can see the main process does very little CPU based processing, it is almost entirely devoted to network IO.  Since we use asyncio for this, it means we can manage a large number of connections simultaneously in one process.  It also means that the main process is shielded from badly behaved workers.

Process Pool
The process pool consists of a set of worker processes that pull one message at a time off of the shared queue.  Each worker unwraps the message, deserializes the data into a command and attempts to either read or write keys and/or values to a shared dictionary.  This is achieved with the use of a read write lock.  The read/write lock allows multiple readers simultaneously and only one write at a time.  Here is an example of how the read write lock works:

R messages are reads and W messages are writes:

R0 - R1 - R2 - R3 - R4 - W5 - R6 - R7 - W8

In the above scenario R0 -> R4 all get executed immediately.  Once they are completed W5 can execute.  If R6 -> W8 execute while W5 is executing, they will need to wait until W5 completes.  Finally, R6 and R7 execute and W8 will need to wait until those two messages complete.


Alternate solutions and improvements:
It is possible to do this without multiple processes.  We could use multi-threading instead which would make the entire problem much simpler.  We wouldn't have to use a queue, we could have a single lock that protects the entire data structure until an update operation is complete.  This has the advantage of being simpler but less scalable in the long run.  With the current multi-process solution things like deserializing the data and any other CPU specific work can happen simultaneously on multiple processes.

Potentially use asyncio in each process instead of blocking.  Or multiple threads in each 'worker' process.  This would provide us even more parallel throughput


We could improve performance a bit by locking keys that are being written instead of the entire database.  It would be more complicated since all writers would have to take locks on all the keys being written (if there are multiple keys).


Run this multithreaded instead of 
Improvements:


Testing


Potential 

