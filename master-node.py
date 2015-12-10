##################
# Master rpc node 
#################
import data_pb2
import time
import commonlib
import master_pb2
import dfss
from grpc.beta import implementations

class MasterNode(master_pb2.BetaMasterNodeServicer):
    def Store(self,request,context):
        """Stores a file on the data node
        Description: This function first queries for a valid data node
        server. This is achieved by loading our config file and
        selecting a server at random, then pinging the server to insure
        its working. Afterwards it makes a gRPC call to the data-node to
        perform the Store() operation. 
        """
        
        #prevents infinite loop
        count = 0
        status = False
        reply_msg = ""
        
         
        if commonlib.DEBUG:
            print("In MasterNode.Store") 
        
        while status == False:
            if count == 5:
                break
            
            #retrieve IP address from loadBalancer
            addr = commonlib.loadBalancer(commonlib.CONFIG).split(":")
            ip = str(addr[0]) #set ip addr
            port = int(addr[1]) #set port number as int
        
            #Ping Data-node to see if it is working
            status = commonlib.isAlive(ip,port)
            #inform which servers is down. 
            server_msg = "Server %s:%d"%(ip,port)
            print(server_msg)
            
            #internal counter to prevent infinite loop
            count = count + 1
        
        if count==5 and status==False:
            reply_msg = "Error has occured, file has not been written"
        else:
            #Establish a connection to the data node. At this point we
            #know that the data-node is alive. 
            channel = implementations.insecure_channel(str(ip),int(port))
            stub = data_pb2.beta_create_DataNode_stub(channel)
            try:
                #rpc call
                reply_msg = "File has been written"    
            except:
                reply_msg = "File has not been written, er -1"
                
        
        
        return master_pb2.StoreReply(reply_msg=request.file_name)

    def Read(self,request,context):
        """Reads a file from data node
        """
      
        if commonlib.DEBUG:
            print("Attempting to read file") 
         
        filename =  request.file_name
        block_size = request.block_size 
        
        count = 0
        status = False
        reply_msg = ""

        while status == False:
            if count == 5:
                break
            
            #retrieve IP address from loadBalancer
            addr = commonlib.loadBalancer(commonlib.CONFIG).split(":")
            ip = str(addr[0]) #set ip addr
            port = int(addr[1]) #set port number as int
        
            #Ping Data-node to see if it is working
            status = commonlib.isAlive(ip,port)
            #inform which servers is down. 
            server_msg = "Server %s:%d"%(ip,port)
            print(server_msg)
            
            #internal counter to prevent infinite loop
            count = count + 1
        
        if count==5 and status==False:
            reply_msg = "Error has occured, file cannot be read. Data nodes couldnt be accessed"
        else:
            #NOTE: Update parameters for calls!
            dfss.Read()
            reply_msg = "File has been read"
        

       
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
