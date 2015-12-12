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
        """Stores a file on the data node, DataNode should make a call
        to our custom DFSS. Therefore it makes a call to dfss.Store()
        after the proper parameters are passed.

        StoreRequest takes the following parameters:
            1. string file_name
            2. bytes file_content 
            3. string timestamp
            4. string user_id

        StoreReply takes the following parameters:
            1. string reply_msg
            2. bool success
        """
         
        if commonlib.DEBUG:
            print("Attempting to access DataNode.Store") 
        
        #test if the request parameters are properly passed, complain if
        #nonetype
        try:
            fn = request.file_name
            fc = request.file_content
            ts = request.timestamp
            uid = request.user_id
        except:
            print("Error in retrieving requests")
            return data_pb2.StoreReply(reply_msg="Err -2",success=False)
        
        #NOTE: Adjust parameters for dfss store operation 
        dfss.Store(fn,fc,ts,uid)
        reply_msg = "File successfully written"

        return data_pb2.StoreReply(reply_msg=request.file_name,success=True)

    def Read(self,request,context):
        """Reads a file from data node. This should simply call
        dfss.Read() with the proper parameters.

        ReadRequest takes the following parameters:
            1. string file_name
            2. string timestamp

        ReadReply takes the following parameters:
            1. bytes reply_file
            2. bool success
        """
       
        if commonlib.DEBUG:
            print("attempting to connect to DataNode Read...")
       
        #ensure that requests are not nonetype
        try:
            fn = request.file_name
            ts = request.timestamp
        except:  
            print("Requests are empty")
            
        #NOTE: Replace with new dfss.Read() 
        fd = dfss.Read(fn,ts)
        fd = str(fd)
        return data_pb2.ReadReply(reply_file=fd,success=True)
    
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
