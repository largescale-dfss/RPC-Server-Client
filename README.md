# RPC-Server-Client
RPCs Server &amp; Client

<h2>Instructions</h2>
In order to start, you must first run the codegen in order to compile
the protocol buffers. We will be using gRPC for python. First execute
the following command:
    ./run_codegen

After the protocol buffers have been generated you can run the RPC
servers. There are two servers. One is called the master-node.py and the
other is called data-node. You should only run one copy of master-node,
however you can run as many data-nodes as possible. The master-node runs
on port 50051 by default. The data-nodes can be ran on any specified
port, by passing a specific port. Execute the following commands:
    python master-node.py
    python data-node.py [port]

The next step is updating the config.txt file to let the master-node
know where exactly is your data-nodes located. For example, if I would
like to specify three data nodes, one running on 50053, 50054, and 50055; I
would do the following [modifications in config.txt]:
    127.0.0.1:50053
    127.0.0.1:50054
    127.0.0.1:50055

Finally run the client file. The client.py provided is a demonstration
file that connects to the master-node and makes a request. The
master-node then handles this request for you. The client supports two
operations a write operation (or store) and a read operation. Depeneding
on the operation is what parameters you pass to the program. 

For a read operation do the following:
    python client.py -r "file_name" "timestamp"

For a write operation do the following:
    python client.py -s "file_name"

For more information on the commands supported by the file you can
execute the following:
    python client.py -h 

