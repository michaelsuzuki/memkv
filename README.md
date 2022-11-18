# memkv

memkv is a simple in memory key value store that supports the following features:
* Get or Set a single key
* Get multiple values for multiple keys
* Set multiple keys to independent values
* Delete a key
* Obtain metrics data about the server

The in-memory store can support multiple clients in parallel and provides a consistent view of the data when a key is simultaneously accessed by multiple clients.


## Prerequisites:

* You must a version of python 3.10+ installed.  Ideally this is done using a virtual environment.
* You must have a version of pip > 22.x installed into the virtual environment or global python 3 distribution
* Protobuf support should be installed.  Instructions are available here: https://developers.google.com/protocol-buffers.  Follow the download and install instructions under "How do I start?"


## Installation:

Unpack the memkv.zip file into a folder
1. cd to the memkv folder
2. Switch to a python 3.10+ virtual environment if necessary
3. run `pip install -r server_requirements.txt`  # If running the server
4. run `pip install -r client_requirements.txt`  # If running the client`
5. run `pip install -r development_requirements.txt` # If working on the code
4. run `protoc -I=. --python-out=${HOME}/{path to this folder}/memkvk/protocol ${HOME}/{path to this folder}/memkv.proto`


## Usage:
### Server:
In order to start the server you need to tell it where the src directory is.  If you are in the base directory of the project you can do the following:
`PYTHONPATH=./src python memkv_server.py`


### Client:
In order to start the client, go to the base directory of the project and run:
`python src/memkv/client/cli.py`

## Testing
`pytest` from the base directory

## Code layout:
* memkv_server.py  # command line script to startup the server
* src/   # The directory where the source lives
* src/memkv/client/api.py  # A library through which a user can call the server
* src/memkv/client/cli.py  # A lightweight cli for interacting with the server
* src/memkv/protocol/memkv_pb2.py # This file is compiled from the memkv.proto in the base directory
* src/memkv/protocol/utils.py # A variety of utility functions to make working with protobufs simpler
* src/memkv/server/locks.py # ReaderWriterLock implementation
* src/memkv/server/server.py # Implementation of the server
* tests/ # The folder that contains the tests
* README.md # This file


## High Level Design
At a high level this is a simple TCP/IP server that listens on a port, accepts clients, parses the request from the client, wraps it in a message and then submits it to a worker. Each worker unwraps the message and determines how it should be executed.

The worker processes the request by reading or writing from a shared Dictionary that is guarded by a Read/Write lock.  The lock allows reads until a write is detected.  Once the write is detected any new workers will need to wait for the write to complete.  The write cannot happen until all previous reads are complete.

Wire Protocol
The wire protocol consists of Google Protocol buffers for efficiency and simplicity.  Every request to the server is encapsulated by a type of Message object.  The message is either a GetCommand, SetCommand, DeleteCommand, MetricsCommand or Response.  The protocol for sending them is quite straightforward.

1. The message is serialized to bytes
2. The length of the bytes is calculated
3. The type of command is encoded as a unsigned short as follows:
   1. GetCommand is 1
   2. SetCommand is 2
   3. DeleteCommand is 3
   4. MetricsCommand is 4
   5. ResponseCommand is 5
5. The first set of bytes is a header that looks like the following C struct:
   struct {
      unsigned short msg_type;
      unsigned long  msg_length;
   }
6. The values are sent in network order before sending the message bytes
7. Next the message bytes are sent

For receiving:
1. Receive the first six bytes and decode them to get the type and the length
2. Receive the message bytes based on the indicated length in the header
3. Deserialize the message bytes using the message type in the header.


The protocol buffer definitions live in the base of the repository in memkv.proto


### Detailed Server Design

The server consists of a main process and a pool of worker threads.  The main processes accepts connections from clients and then reads the data asynchronously and wraps it with a MessageWrapper (which holds the header and the serialized command).  Each message is then added to a queue which is consumed by the worker processes.  It uses asyncio to allow maximum io throughput in the main process.

#### The Main Process
The main process is responsible for:
* Accepting client sockets
* Reading requests off of those sockets
* Pushing the requests into a ThreadPool for processing
* Holding a reference to the hash table.
* Manages a pool of threads that run the workers that consume the TCP streams

As you can see the main process does very little CPU based processing, it is almost entirely devoted to network IO.  Since we use asyncio for this, it means we can manage a large number of connections simultaneously in one process.  It also means that the main process is shielded from badly behaved workers.

#### Thread Pool
The thread pool consists of a set of worker processes that are dedicated to running tasks for each client.  Each worker unwraps the message, deserializes the data into a command and attempts to either read or write keys and/or values to a shared dictionary.  This is achieved with the use of a read write lock.  The read/write lock allows multiple readers simultaneously and only one write at a time.  Here is an example of how the read write lock works:

R messages are reads and W messages are writes:

R0 - R1 - R2 - R3 - R4 - W5 - R6 - R7 - W8

In the above scenario R0 -> R4 all get executed immediately.  Once W5 comes in, it takes a lock over the entire hash table allowing no other access.  Once it is done other tasks may access the hash table until another write comes in.  Thus, we are prioritizing writes over reads.

Because of the read write lock, we don't want this in the asyncio path and why we instead want this work parceled out to a thread pool.  We don't want to block the thread that is handling all of our IO asynchronously.


## Alternate solutions and improvements:
Currently we use a read write lock and prioritize writes.  This could lead to a starvation scenario when a lot of writes come in blocking reads.  Given that reads are supposed to significantly outweigh writes that is probably not a big issue.

We could do finer grained locking so that if there are no reads on a particular key, a lock could be taken on that key and allow readers to access everything else.  This would be fairly costly and if there is a situation where there are multiple keys being updated at the same time we would need to figure out how to lock a bunch of keys at the same time (maybe a condition variable that checks a set of keys?).

We could've used blocking IO instead of asyncio.  However, given this is a standalone server, the ability to handle large number of clients would make asyncio a more scalable choice in this instance.

Why not http?  I considered http, but it has a lot of overhead in terms of what gets sent over the wire even if compressed.  Assuming we want a high throughput service, I believe that protocol buffers would be much faster and more efficient over the wire.  I belive the biggest issue is the difficulty to debug due to the binary format.

Currently, this service is completely in the clear over the wire.  Ideally we would want to add ssl support.  Based on what I can tell that would not be to difficult to add in the future.





