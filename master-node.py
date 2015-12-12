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
        
        StoreRequest takes the following parameters:
            1. string file_name
            2. bytes file_content
            3. string timestamp
            4. optional string user_id
        
        StoreReply returns the following:
            1. string reply_msg
            2. bool success
        """
        
        #prevents infinite loop
        count = 0
        status = False

        # these will be passed as StoreReply
        reply_msg = ""
        success = False 
         
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
            if commonlib.DEBUG:
                print(server_msg)
            
            #internal counter to prevent infinite loop
            count = count + 1
        
        #if load balancer cannot find an avaialble data node
        #return with error message
        if count==5 and status==False:
            reply_msg = "Error has occured, file has not been written"
            print("Could not load valid ip addr for data node")
            return master_pb2.StoreReply(reply_msg=reply_msg,success=False) 
        
        else:
            #Establish a connection to the data node. At this point we
            #know that the data-node is alive. 
            channel = implementations.insecure_channel(str(ip),int(port))
            stub = data_pb2.beta_create_DataNode_stub(channel)
            try: 
                #parameters being passed to request
                pfn = request.file_name
                pfc = request.file_content
                pts = request.timestamp
                puid = request.user_id 
                
                req = data_pb2.StoreRequest(file_name=pfn,file_content=pfc,timestamp=pts,user_id=puid)
                response = stub.Store(req,commonlib.TIMEOUT)
                #response = stub.Read(data_pb2.ReadRequest(user_name="",file_name="",timestamp="",block_size=0),commonlib.TIMEOUT)
                
                reply_msg = "File has been written" 
                if commonlib.DEBUG:
                    print(reply_msg)
                
                #set success flag
                success=True 

            except:
                reply_msg = "File has not been written, er -1"
                        
        return master_pb2.StoreReply(reply_msg=reply_msg,success=success)

    def Read(self,request,context):
        """Reads a file from data node.
        
        ReadRequest takes the following parameters:
            1. string file_name
            2. string timestamp
            
        ReadReply takes the following parameters:
            1. bytes reply_file
            2. bool success 
        """
      
        if commonlib.DEBUG:
            print("Attempting to read file") 
        
        try: 
            fn =  request.file_name
            ts = request.timestamp
        except:
            print("Failed to retrieve request params")

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
            return master_pb2.ReadReply(reply_file="",success=False)
        else:
            #NOTE: Update parameters for calls!
            fd = dfss.Read(fn,ts)
            reply_msg = "File has been read"
            fd = str(fd)

        return master_pb2.ReadReply(reply_file=fd,success=True)


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
