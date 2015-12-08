import os
from grpc.beta import implementations
import random
#global definitions used throughout program
MB = 1<<20
TIMEOUT = 10+10 
DEBUG = True
CONFIG = "config.txt" #config file


def splitFile(fd,block_size):
    """Takes a string of a file and splits file into array based on
    block size.

    fd - string of desired file to split
    Block_size - desire block size to split file
    """

    #new_block is a new array that contains the 
    #contents of the file, split based on block_size
    new_block = []
    with open(fd,'rb') as f:
        while True:
            block = f.read(block_size)
            if block:
                new_block.append(block)
            else:
                break
        
    return new_block


def loadBalancer(config_file):
    """Loads cf and determines which IP address has best ping.
    Uses random policy. Picks a server based on a 'coin flip'
    and then sends a probe to see if the server is alive, if so, then
    the IP addr of this server is sent back. This is a naive approach
    for a load balancer.
    @config_file - text file with the following format <ip addr>:<port>
                example: 127.0.0.1:50051
                         127.0.0.1:50052
                         127.0.0.1:50053
    """ 
    ip_addresses = []
    best_ip = ""
    with open(config_file, "r") as config_txt:
        for ip_addr in config_txt:
            ip_addresses.append(ip_addr)
    

    
    # coin flip, to select a random index to choose from
    r = random.randint(0,len(ip_addresses)-1)
    
    #best_ip is just a random ip from config_file
    best_ip = ip_addresses[r]
    if DEBUG:
        print("List of addresses"+str(ip_addresses))
        print("Best IP: " + str(best_ip))
    return best_ip
