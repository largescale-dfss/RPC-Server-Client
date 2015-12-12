#################################
# Distributed File System Calls
################################
import os
import sys


def Store(file_name,file_content,timestamp,user_id):
    new_file = "%s@%s" % (file_name,timestamp)
    fd = open(new_file,"w+")
    for i in range(len(file_content)):
        fd.write(file_content[i])
    fd.close() 
    return True

def Read(file_name,timestamp):
    print("Reading in dfss.Read")
    #note this function doesnt support timestamp yet.
    fd = open(file_name,"r")
    lines = fd.readlines()
    blocks = []
    for line in lines:
        blocks.append(line)
    return blocks 
