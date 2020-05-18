#import required libraries
import logging
import os
import subprocess
from kazoo.client import KazooClient
from kazoo.client import KazooState
import time
time.sleep(6)

zk_path="/producer/" #root path for zookeeper watch is specified here
data = os.environ
print(data,"\nos.environ\n")
wid=os.environ['workerUniqueId'] #fetching the environment variable value for this worker 
print(os.environ['workerUniqueId'],"\nenv id\n")
newZnodePath=zk_path+"Worker"+str(wid) #forming the path for this znode
zk = KazooClient(hosts='zoo:2181') #connecting to zookeeper server
zk.start()
zk.ensure_path(zk_path) #ensures a path , creates if necessary

# It checks whether the newZnodePath exists 
# and creates if neccessary marking it as ephemeral
# which indicates that if connection is lost then the znode
# is removed by the zookeeper 
if zk.exists(newZnodePath):
    print("Node already exists")
else:
    print("Creating new znode")
    zk.create(newZnodePath, b"master",ephemeral=True)

#Starts a python process which makes the worker to behave as master
workerProc=subprocess.Popen(["python","master.py","1"])


logging.basicConfig()

# This function is called whenever the data is changed 
# w.r.t this znode in case of master election.
# Checks whetehr the set data is master indicating this
# znode as the new master, in that case python process 
# is killed to make it behave as master.
def demo_func(event):
    global zk_path
    global workerProc
    global zk
    data, stat = zk.get(newZnodePath,watch=demo_func)
    mydata=data.decode("utf-8")
    if(mydata=="master"):
        print("Yay !! I am the new master now. says: "+newZnodePath)
        print("restart the process worker.py")
        workerProc.kill()
        workerProc=subprocess.Popen(["python","master.py","1"])
        workerProc.wait()

    print(event)
    children = zk.get_children(zk_path)
    print("There are %s children with names %s" % (len(children), children))


data, stat = zk.get(newZnodePath,watch=demo_func)
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
workerProc.wait()
