##################
# Master rpc node 
#################
import data_pb2
import time
import commonlib
import master_pb2
from grpc.beta import implementations

class MasterNode(master_pb2.BetaMasterNodeServicer):
    def Store(self,request,context):
        """Stores a file on the data node
        """
        
        reply_msg = "Error has occured, file has not been written"
     
        
        return master_pb2.StoreReply(reply_msg=request.file_name)

    def Read(self,request,context):
        """Reads a file from data node
        """
      
        if commonlib.DEBUG:
            print("Attempting to read file") 
         
        filename =  request.file_name
        block_size = request.block_size 
       
        #picks a random ip addr from config file, we need these fields
        #so we can: 
        #1. ping data node to see if it is alive
        #2. if data node is avaiable, read from this node.   
        addr = commonlib.loadBalancer(commonlib.CONFIG).split(":")
        ip = addr[0] #set ip addr as string
        port = int(addr[1]) #set port number as int
        if commonlib.DEBUG:
            print(ip)
            print(port)  
        channel = implementations.insecure_channel('127.0.0.1',50052)
        stub = data_pb2.beta_create_DataNode_stub(channel)
        print("yay we got here") 
       
        #attempt to ping server
       #NOTE: Add loop so we can try other servers in config file until
       #a response is found
        try:
            print("attempting to connect to isAlive..")
            response = stub.isAlive(data_pb2.AliveRequest(ping=True),commonlib.TIMEOUT)
            print("Yay server is alive!")
        except:
            print("The server is possibly not alive... Please try again..")
            exit()
        #if the block_size flag has not been set, set it to the default
        #1mb
        if block_size == 0:
            block_size = commonlib.MB 
        
        fd = commonlib.splitFile(filename,block_size)
        if commonlib.DEBUG:        
            print("reading file ")
        
        return master_pb2.ReadReply(reply_file=fd[0])


def main():
    """Creates Master Node server and listens onto port 50051"""
    print("\n\tStarting server on localhost:50051...")
    server = master_pb2.beta_create_MasterNode_server(MasterNode())
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(commonlib.TIMEOUT)
    except KeyboardInterrupt:
        print("\n\tKilling the server...\n")
        server.stop(grace=0)
        exit()
    except:
        print("Error has occured, please refer to log")



if __name__ == '__main__':
    main()
