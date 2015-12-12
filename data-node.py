##################
# Data rpc node 
#################

import time
import commonlib
import data_pb2
import dfss
from grpc.beta import implementations
import sys 
class DataNode(data_pb2.BetaDataNodeServicer):
    
    def Store(self,request,context):
        """Stores a file on the data node
        """
        
        reply_msg = "Error has occured, file has not been written"
        
        if commonlib.DEBUG:
            print("Attempting to access DataNode.Store") 
        try:
            if commonlib.DEBUG:
                print("Attempting to print file requests in DataNode.Store")
            fn = request.file_name
            fc = request.file_content
            ts = request.timestamp
            uid = request.user_id
            print("FC= %s ; Timestamp=%s ; uid=%s" % (fn,ts,uid))
        except:
            print("Error in retrieving requests")
            return data_pb2.StoreReply(reply_msg="Err -2",success=False)
        
        #NOTE: Adjust parameters for dfss store operation 
        dfss.Store(fn,fc,ts,uid)
        
        return data_pb2.StoreReply(reply_msg=request.file_name,success=True)

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
        
        
        if commonlib.DEBUG:
            print("reading file ")
        
        dfss.Read()

        return data_pb2.ReadReply(reply_file=fd[0])
    
    def isAlive(self,request,context):
        """This responds with a message indicating the service is alive.
        """    
        
        #msg = "This service is alive"
        
        return data_pb2.AliveReply(health=True) 

def main():
    """Creates Master Node server and listens onto port according to
    commandline arg"""
    if len(sys.argv)  == 1:
        print("Please pass in a port number to run!")
    #set port 
    port = sys.argv[1]

    print("\n\tStarting server on localhost:"+port)
    server = data_pb2.beta_create_DataNode_server(DataNode())
    ip = "[::]:"+str(port)
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
