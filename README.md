# RPC-Server-Client
RPCs Server &amp; Client

<h2>Instructions</h2>
    First execute:
        ./run_codegen
    Then data-node.py and master-node.py files should be executed. You
    can run as many data-nodes as you desire, as long as you add the ip
    and port of each data-node to the config.txt. The format for this
    should be <ip>:<port> for example: 127.0.0.1:50053. 
    Lastly the client should be executed.  You can execute the
    following:
        python client.py -h 
    for a more verbose listing of options avaliable. 
