#################################
# Distributed File System Calls
################################
import os
import sys


def Store(file_name,file_content,timestamp,user_id):
    new_file = "%s:%s" % (file_name,timestamp)
    fd = open(new_file,"w+")
    for i in range(len(file_content)):
        fd.write(file_content[i])
    fd.close() 
    return True

def Read():
    print("This function has yet to be implemented")
    return False
