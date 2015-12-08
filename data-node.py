##################
# Data rpc node 
#################

import time
import commonlib
import data_pb2
from grpc.beta import implementations
import sys 
class DataNode(data_pb2.BetaDataNodeServicer):
    
    def Store(self,request,context):
        """Stores a file on the data node
        """
        
        reply_msg = "Error has occured, file has not been written"
     
        
        return master_pb2.StoreReply(reply_msg=request.file_name)

    def Read(self,request,context):
        """Reads a file from data node
        """
       
        if commonlib.DEBUG:
            print("attempting to connect to DataNode Read...")
        filename =  request.file_name
        block_size = request.block_size 
       
        #if the block_size flag has not been set, set it to the default
        #1mb
        if block_size == 0:
            block_size = commonlib.MB 
        
        fd = commonlib.splitFile(filename,block_size)
        
        print("reading file ")
        return data_pb2.ReadReply(reply_file=fd[0])
    
    def isAlive(self,request,context):
        """This responds with a message indicating the service is alive.
        """    
        msg = "This service is alive"
        if commonlib.DEBUG:
            print("You are trying to check to see if i'm alive")
        return data_pb2.AliveReply(health=True,reply_msg=msg) 

def main():
    """Creates Master Node server and listens onto port according to
    commandline arg"""
    if len(sys.argv)  == 1:
        print("Please pass in a port number to run!")
    #set port 
    port = sys.argv[1]

    print("\n\tStarting server on localhost:"+port)
    server = data_pb2.beta_create_DataNode_server(DataNode())
    ip = "localhost:"+str(port)
    server.add_insecure_port(ip)
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
