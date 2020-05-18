#import required libraries
import sqlite3 
from flask import Flask, render_template,jsonify,request,abort,Response 
import requests 
import json 
import csv
import pika
import time 
import uuid
from datetime import datetime
import docker
import threading
import math


import logging

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import EventType


logging.basicConfig()

time.sleep(15)
app=Flask(__name__)

#connecting to zookeeper server
zk = KazooClient(hosts='zoo:2181')
zk.start()


crashSlaveApiCalled=False
workerCount=1
prevZnodeDataWatchCalledOn=""
prevEventType=""

zk_path="/producer/" #root path for zookeeper watch is specified here
zk.ensure_path(zk_path)  #ensures a path , creates if necessary
scalingDown=False
allZnodes={}
znodesCount=0
pidZnodeMapping={}
currentMasterZnodePath="/producer/Worker0" #the master path at starting
currentMasterpid=0
slavesDeletedDueToScaleDown=0

# This funtion is called as soon as the orchestrator is started which keeps the 
# mapping containing the pid and id of containers of two initial workers is kept.  
def initialisePidZnodeMapping():
    print("\n\nInitialising pidZnodeMapping . . .")
    global client
    container_list = []
    x = client.containers.list(filters={"ancestor": "master_image"})
    y = client.containers.list(filters={"ancestor": "slave_image"})
    print(x, "containers")
    print(x[0])
    container_list.append(x[0].id)
    container_list.append(y[0].id)
    pidList = []
    c = docker.APIClient()
    d = 0
    for i in container_list:
            stat = c.inspect_container(i)
            pidList.append(stat['State']['Pid'])
    print(pidList, "pid list")
    print(pidList,"Worker PidList")
    pidZnodeMapping[pidList[0]]="/producer/Worker0"
    pidZnodeMapping[pidList[1]]="/producer/Worker1"

#specifies the time in seconds to wait before calling the daemon process
WAIT_SECONDS=120

#instantiating the client to talk to the docker daemon
client = docker.from_env()

#This function spawns new containers when needed in case of scaling out or fault tolerance
#the new container is run with specified network so that it can communicate with other containers and
#detach=true indicates that the container will run in the background.
#pidZnodeMapping is maintained which is used in case of master election.
@app.route("/api/v1/spawn/slave",methods=["POST"])
def spawn_slave():
	count=request.get_json()["count"]
	global workerCount
	global pidZnodeMapping
	print(count,"number of new containers to be spawned")
	for i in range(count):
		workerCount+=1
		container=client.containers.run("slave_image",environment=["workerUniqueId="+str(workerCount)],network="cc_local_copy_network",detach=True)
		contId=container.id
		apiClient=docker.APIClient()
		data = apiClient.inspect_container(contId)
		contPid=data['State']['Pid']
		print(contPid,"PID of new Container spawned")
		pidZnodeMapping[contPid]="/producer/Worker"+str(workerCount)
		print(pidZnodeMapping)
	return "success"

#This function is called when a read request is called to increment the count of 
#read requests.For the first time it sets the second line value to zero so that the 
#daemon process is called and it is set to 1 on further requests to prevent it from calling
#daemon process.
def fun_for_count():
	try:
		file=open("read_count.txt","r")
		e=file.readline()
		q=int(e)+1
		file.close()
		file=open("read_count.txt","w")
		file.write(str(q))
		file.write("\n")
		file.write("1")
	except:
		print("first time called")
		file=open("read_count.txt","w")
		file.write("1")
		file.write("\n")
		file.write("0")
	file.close()

#This is the funtion which checks the count of read requests every two minutes.
#Based upon the count the decision of scaling out or scaling in is taken.
#In case of scale_out spawn/slave endpoint is called and in case of scale_in crash/slave endpoint is called.
#It cheks the current number of slaves by calling the worker list and compares it with the containers needed to 
# take the decison of scale_in or scale_out.And it resets the count to zero.
def daemon_call():
	global scalingDown
	global  slavesDeletedDueToScaleDown
	file=open("read_count.txt","r")
	read_line=file.readline()
	count=int(read_line)
	file.close()
	res=requests.get("http://0.0.0.0:5000/api/v1/worker/list")	
	print(json.loads(res.text),"worker list")
	number_of_cont=len(json.loads(res.text))-1
	new_containers=math.ceil(count/20)
	if(new_containers==0):
		new_containers=1
	if(new_containers>number_of_cont):
		scale_out=new_containers-number_of_cont
		result=requests.post("http://0.0.0.0:5000/api/v1/spawn/slave",json={"count":scale_out})
	elif(number_of_cont>new_containers):
		scale_in=number_of_cont-new_containers
		for i in range(scale_in):
			slavesDeletedDueToScaleDown+=1
			scalingDown=True
			result=requests.post("http://0.0.0.0:5000/api/v1/crash/slave",json={"reason":"scale_in"})
		scalingDown=False
	
	file=open("read_count.txt","w")
	print("resetting count")
	file.write("0")
	file.write("\n")
	file.write("1")
	file.close()
	threading.Timer(WAIT_SECONDS, daemon_call).start()

#This function elects a new leader from the slave workers which has got the lowest pid.
#It call worker/list end point to get all the sorted list of worker pids from that it will pick the
#first pid and with the help of mapping we will set the containers data to master which triggers a 
#watch related to that container.
def electLeader():
    global zk
    global currentMasterZnodePath
    global currentMasterpid
    pids = requests.get(url='http://0.0.0.0:5000/api/v1/worker/list')
    pidList = json.loads(pids.text)
    newMasterPid=pidList[0]
    currentMasterZnodePath=pidZnodeMapping[newMasterPid]
    print("\nThis guy is new master",newMasterPid)
    print("This guy is new master",currentMasterZnodePath)
    currentMasterpid=newMasterPid
    zk.set(currentMasterZnodePath,b"master")
    print("Creating slave after electing leader")
    result=requests.post("http://0.0.0.0:5000/api/v1/spawn/slave",json={"count":1})

#This function checks whether master died by comparing it with the currentZnodePath which
#will be containing the current masters znode path and it that's true then electLeader function 
#is called.Else it is obvious that some slave died, so in response to that a slave worker is spawned 
# only if the slave worker is not dead due to scaling down.
def checkIfMasterDied(event):
    global currentMasterZnodePath
    global scalingDown
    global slavesDeletedDueToScaleDown
    print('\n\nchecking if master died . . .')
    print(event.path,"Event.path")
    if(event.path==currentMasterZnodePath):
        print("[x] Checking done.")
        print("Yes its true that master died :(")
        print("Lets elect our new leader")
        electLeader()
    else:
        print("Master didn't die :)")
        if slavesDeletedDueToScaleDown==0:
            print("creating slave due to fault tolerance")   
            result=requests.post("http://0.0.0.0:5000/api/v1/spawn/slave",json={"count":1})
        if(slavesDeletedDueToScaleDown>0):
            slavesDeletedDueToScaleDown-=1

#This function keeps a watch on the particular znode supplied as the argument to the function.
#It checks the event type to check whether znode got deleted or not. If it is deleted then 
#checkIfMasterDied function is called.
def foo(znode):
    @zk.DataWatch("/producer/"+znode)
    def watch_children_data(data, stat, event):
        print("\nDataWatch Called\n")
        print(data,"data\n")
        print(event,"event\n")
        print(stat,"stat\n")
        global prevEventType
        global prevZnodeDataWatchCalledOn
        
        if(event!=None):
            if(prevEventType=="deleted" and prevZnodeDataWatchCalledOn==event.path):
                return 
            else:
                if(event.type==EventType.DELETED):
                    
                    prevEventType="deleted"
                    prevZnodeDataWatchCalledOn=event.path
                    print("Some worker got deleted!")
                    checkIfMasterDied(event)

#A children wathc kept on the root /producer/ to detect whether any znode got deleted or not.
#Depending upon the that particular action will be taken.Whenever the funtion is triggered then 
#foo funtion is called for every children.
@zk.ChildrenWatch("/producer/")
def watch_children(children):
    global crashSlaveApiCalled
    global allZnodes    
    global znodesCount
    global scalingDown
    print("\nchildrenWatch called\n")
    print("\n\ncrashSlaveApiCalled",crashSlaveApiCalled)
    print("\n\nIn orchestrator watch , Children are now: %s" % children)
    for znode in children:
        # if(znode not in allZnodes):
        #     allZnodes[znode]=0
            foo(znode)
            znodesCount+=1
    

class write_class(object):
	
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
		self.channel = self.connection.channel()
		result=self.channel.queue_declare(queue='', exclusive=True)
		self.callback_queue_write=result.method.queue
		self.channel.basic_consume(queue=self.callback_queue_write,
		on_message_callback=self.on_response_write,
		auto_ack=True)

	def on_response_write(self, ch, method, props, body):
		print("inside on_response",body)
		print(props.correlation_id,"resturned id")
		if self.corr_id == props.correlation_id:
			self.write_response = json.loads(body)
	
	def write_call(self, n):
		self.write_response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange='',
		routing_key='write_queue',
		properties=pika.BasicProperties(
			reply_to=self.callback_queue_write,
			correlation_id=self.corr_id,
		),
		body=json.dumps(n))
		while self.write_response is None:
			self.connection.process_data_events()
		return self.write_response

class read_class(object):
	
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
		self.channel1 = self.connection.channel()
		result=self.channel1.queue_declare(queue='', exclusive=True)
		self.callback_queue_read=result.method.queue
		self.channel1.basic_consume(queue=self.callback_queue_read,
		on_message_callback=self.on_response_read,
		auto_ack=True)

	def on_response_read(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.read_response = json.loads(body)
	
	def read_call(self, n):
		self.read_response = None
		self.corr_id = str(uuid.uuid4())
		self.channel1.basic_publish(exchange='',
		routing_key='read_queue',
		properties=pika.BasicProperties(
			reply_to=self.callback_queue_read,
			correlation_id=self.corr_id,
		),
		body=json.dumps(n))
		while self.read_response is None:
			self.connection.process_data_events()
		return self.read_response


@app.route("/api/v1/db/write",methods=["POST"])
def write_database():
	write_content=request.get_json()
	obj=write_class()
	write_response = obj.write_call(write_content)
	print(write_response,"final response")
	return jsonify(write_response)

@app.route("/api/v1/db/read",methods=["POST"])
def read_database():
	fun_for_count()
	file=open("read_count.txt","r")
	count=file.readline()
	print(count,"number of requests")
	count2=file.readline()
	call_daemon=int(count2)
	file.close()
	if(call_daemon==0):
		print("calling daemon process")
		threading.Timer(WAIT_SECONDS,daemon_call).start()
	read_content=request.get_json()
	read_obj=read_class()
	read_response=read_obj.read_call(read_content)
	return jsonify(read_response)


@app.route("/api/v1/worker/list",methods=["GET"])
def worker_list():
	container_list=[]
	x=client.containers.list()
	print(x,"containers")
	for i in x:
		if(i.name=="orchestrator" or i.name=="rabbitmq" or i.name=="cc_local_copy_zoo_1"):
			print(i.id,i.name)
		else:
			container_list.append(i.id)
	pid_list=[]
	c=docker.APIClient()
	for i in container_list:
		stat=c.inspect_container(i)
		pid_list.append(stat['State']['Pid'])
	pid_list.sort()
	print(pid_list,"sorted list")
	return json.dumps(pid_list),200

@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
	slave_list=[]
	mapping={}
	x=client.containers.list(filters={"ancestor":"slave_image"})
	print(x,"slave containers")
	for i in x:
		slave_list.append(i.id)
	slave_pid=[]
	c=docker.APIClient()
	for i in slave_list:
		stat=c.inspect_container(i)
		PID=(stat['State']['Pid'])
		slave_pid.append(PID)
		mapping[PID]=i
	slave_pid.sort()
	print(mapping,"mapping dictionary")
	if(len(slave_pid)==0):
		print("no containers to kill")
	largest_pid=slave_pid[len(slave_pid)-1]
	to_be_killed=mapping[largest_pid]
	for j in x:
		if(j.id==to_be_killed):
			j.stop()
			j.remove()
	return json.dumps([largest_pid]),200


@app.route("/api/v1/crash/master", methods=["POST"])
def crash_master():
        
        print("killing master")
        global scalingDown
        global crashSlaveApiCalled
        slave_list = []
        crashSlaveApiCalled=True
        mapping = {}
        if(currentMasterpid==0):
            x = client.containers.list(filters={"ancestor": "master_image"})
        else:
            x = client.containers.list(filters={"ancestor": "slave_image"})
        print(x, "worker containers")
        for i in x:
            slave_list.append(i.id)
        slave_pid = []
        c = docker.APIClient()
        if(currentMasterpid==0):
            for i in slave_list:
                stat = c.inspect_container(i)
                PID = (stat['State']['Pid'])
                slave_pid.append(PID)
                mapping[PID] = i
        else:
            for i in slave_list:
                stat = c.inspect_container(i)
                PID = (stat['State']['Pid'])
                if(PID==currentMasterpid):
                    slave_pid.append(PID)
                    mapping[PID] = i
                    
        slave_pid.sort()
        print(mapping, "Killing master")
        if(len(slave_pid) == 0):
            print("no containers to kill")
        largest_pid = slave_pid[len(slave_pid)-1]
        to_be_killed = mapping[largest_pid]
        for j in x:
            if(j.id == to_be_killed):
                j.stop()
                exitCode=j.wait()
                print("\nExitcode",exitCode)
        
        return json.dumps(largest_pid)

@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	if(request.method!="POST"):
		abort(405,"method not allowed")
	
	res=requests.post("http://0.0.0.0:5000/api/v1/db/write",json={"indicate":"3"})	
	if(res.json()==0):
		abort(400,"failed to clear")
	elif(res.json()==1):
		return json.dumps({'success':"cleared successfully"}), 200, {'ContentType':'application/json'}



if __name__ == '__main__':
	app.debug=True
	initialisePidZnodeMapping()
	app.run(host='0.0.0.0',port=5000,use_reloader=False)
