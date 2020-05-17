import logging
import os
import subprocess
from kazoo.client import KazooClient
from kazoo.client import KazooState
import time
from kazoo.client import KazooState
time.sleep(14)

zk_path="/producer/"
data = os.environ
print(data,"\nos.environ\n")
wid=os.environ['workerUniqueId']
print(os.environ['workerUniqueId'],"\nenv id\n")
newZnodePath=zk_path+"Worker"+str(wid)
zk = KazooClient(hosts='zoo:2181')
zk.start()
zk.ensure_path(zk_path)
if zk.exists(newZnodePath):
    print("Node already exists")
else:
    print("Creating new znode")
    zk.create(newZnodePath, b"slave",ephemeral=True)
workerProc=subprocess.Popen(["python","slave.py","0"])


logging.basicConfig()

# Can also use the @DataWatch and @ChildrenWatch decorators for the same
def demo_func(event):
    # Create a node with data
    global zk_path
    global workerProc
    global zk
    data, stat = zk.get(newZnodePath,watch=demo_func)
    mydata=data.decode("utf-8")
    if(mydata=="master"):
        #restart the process worker.py
        print("Yay !! I am the new master now. says: "+newZnodePath)
        print("restart the process worker.py")
        workerProc.kill()
        workerProc=subprocess.Popen(["python","slave.py","1"])
        workerProc.wait()
    # zk.create("/producer/node_2", b"new demo producer node") 
    print(event)
    children = zk.get_children(zk_path)
    print("There are %s children with names %s" % (len(children), children))


data, stat = zk.get(newZnodePath,watch=demo_func)
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
workerProc.wait()
