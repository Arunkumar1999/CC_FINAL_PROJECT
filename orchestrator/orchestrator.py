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
lock = threading.Lock()

import logging

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import EventType


logging.basicConfig()

time.sleep(15)
app=Flask(__name__)


zk = KazooClient(hosts='zoo:2181')
zk.start()
spawn_new=1

c=0
workerCount=1
prevZnodeDataWatchCalledOn=""
prevEventType=""

zk_path="/producer/"
zk.ensure_path(zk_path)
scalingDown=False
allZnodes={}
znodesCount=0
pidZnodeMapping={}
currentMasterZnodePath="/producer/Worker0"
currentMasterpid=0
slavesDeletedDueToScaleDown=0
#def demo_func(event):
#    print("somethings changed in slave")

#data, stat = zk.get("/slave/node_1")
#print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

# List the children
#children = zk.get_children("/slave", watch=demo_func)
#print("There are %s children with names %s" % (len(children), children))
# @zk.ChildrenWatch("/slave")
# def zookeep(children):
# 	global spawn_new
# 	if(spawn_new==2):
# 		print("I AM TRIGGERED BECAUSE  A CHILD IS DEAD :(")
# 		client.containers.run("slave_image",command="python slave.py 0",network="cc_final_test_network",detach=True)
# 		spawn_new=1
# 	else:
# 		return "nothing should be done"

def initialisePidZnodeMapping():
    print("\n\nInitialising pidZnodeMapping . . .")
    # pids = requests.get(url='http://127.0.0.1/api/v1/worker/list')
    # pids=worker_list()
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
    # pidList =list( pids)
    # newMasterPid=pidList[0]
    print(pidList,"Worker PidList")
    pidZnodeMapping[pidList[0]]="/producer/Worker0"
    pidZnodeMapping[pidList[1]]="/producer/Worker1"

WAIT_SECONDS=120
#spawn_new=1
client = docker.from_env()

@app.route("/api/v1/spawn/slave",methods=["POST"])
def spawn_slave():
	count=request.get_json()["count"]
	global workerCount
	global pidZnodeMapping
	print(count,"number of new containers to be spawned")
	for i in range(count):
		workerCount+=1
		client.containers.run("slave_image",environment=["workerUniqueId="+str(workerCount)],network="cc_final_test_network",detach=True)
		print(container)
        contId=container.id
        apiClient=docker.APIClient()
        data = apiClient.inspect_container(contId)
        contPid=data['State']['Pid']
        print(contPid,"PID of new Container spawned")
        pidZnodeMapping[contPid]="/producer/Worker"+str(workerCount)
        # x = client.containers.list()
        print(pidZnodeMapping)
	return "success"

def fun_for_count():
	try:
		#lock.acquire()
		file=open("read_count.txt","r")
		e=file.readline()
		q=int(e)+1
		file.close()
		file=open("read_count.txt","w")
		#file.write(str(q)+" 1")
		file.write(str(q))
		file.write("\n")
		file.write("1")
		#lock.release()
	except:
		#lock.acquire()
		print("inside except")
		file=open("read_count.txt","w")
		#file.write("%d %d"%(1,0))
		file.write("1")
		file.write("\n")
		file.write("0")
		#lock.release()
	file.close()

def deamon_call():
	#lock.acquire()
	global scalingDown
    global  slavesDeletedDueToScaleDown
	file=open("read_count.txt","r")
	read_line=file.readline()
	print(read_line,"reading file dsfdsf")
	count=int(read_line)
	read_line2=file.readline()
	check_initial=int(read_line2)
	print(check_initial,"second digit in file")
	file.close()
	#lock.release()
	print(count,"gggg")
	res=requests.get("http://0.0.0.0:5000/api/v1/worker/list")
	print(res)	
	print(json.loads(res.text),len(json.loads(res.text)),"aaaaa")
	number_of_cont=len(json.loads(res.text))-1
	new_containers=math.ceil(count/20)
	if(new_containers==0):
		new_containers=1
	if(new_containers>number_of_cont):
		scale_out=new_containers-number_of_cont
		#for i in range(scale_out):
		#	client.containers.run("slave_image",command="python slave.py",network="cc_final_test_network",detach=True)
		result=requests.post("http://0.0.0.0:5000/api/v1/spawn/slave",json={"count":scale_out})
	elif(number_of_cont>new_containers):
		scale_in=number_of_cont-new_containers
		for i in range(scale_in):
			slavesDeletedDueToScaleDown+=1
        	scalingDown=True
			result=requests.post("http://0.0.0.0:5000/api/v1/crash/slave",json={"reason":"scale_in"})
		scalingDown=False
	
	#for i in range()
	#	print("inside foo function")
	#	client.containers.run("slave_image",command="python slave.py",network="cc_final_test_network",detach=True)
	#	print("new container created")
	#lock.acquire()
	file=open("read_count.txt","w")
	if(check_initial==0):
		print("writing 1 1 intially")
		#file.write("%d %d"%(1,1))
		file.write("1")
		file.write("\n")
		file.write("1")
	else:
		print("resetting count")
		#file.write("%d %d"%(0,1))
		file.write("0")
		file.write("\n")
		file.write("1")
	file.close()
	#lock.release()
	threading.Timer(WAIT_SECONDS, deamon_call).start()

def electLeader():
    global zk
    global currentMasterZnodePath
    global currentMasterpid
    pids = requests.get(url='http://127.0.0.1/api/v1/worker/list')
    print(pids,"Pids")
    pidList = json.loads(pids.text)
    newMasterPid=pidList[0]
    currentMasterZnodePath=pidZnodeMapping[newMasterPid]
    print("\nThis guy is new master",newMasterPid)
    print("This guy is new master",currentMasterZnodePath)
    currentMasterpid=newMasterPid
    zk.set(currentMasterZnodePath,b"master")
    print("Creating slave after electing leader")
    createSlaves(1)


def checkIfMasterDied(event):
    global currentMasterZnodePath
    global scalingDown
    global slavesDeletedDueToScaleDown
    print('\n\nchecking if master died . . .')
    print(event.path,"Event.path")
    if(event.path==currentMasterZnodePath):
        print("[x] Checking done.")
        print("Yes its true that master died :(")
        print("Lets elect our new king .")
        electLeader()
    else:
        print("Chill dude ! Master dint die :)")
        if slavesDeletedDueToScaleDown==0:
            print("creating slave due to fault tolerance")   
            createSlaves(1)
        if(slavesDeletedDueToScaleDown>0):
            slavesDeletedDueToScaleDown-=1
            # scalingDown=False


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
                    print("Some worker went to hell!")
                    checkIfMasterDied(event)

@zk.ChildrenWatch("/producer/")
def watch_children(children):
    global crashSlaveApiCalled
    global allZnodes    
    global znodesCount
    global scalingDown
    global c
    if(c>=2):
        c=0
    # Vishal code
    #end :)
    print("\nchildrenWatch called\n")
    print("\n\ncrashSlaveApiCalled",crashSlaveApiCalled)
    print("\n\nIn orchestrator watch , Children are now: %s" % children)
    for znode in children:
        if(znode not in allZnodes):
            # allZnodes.append(znode)
            allZnodes[znode]=0
            foo(znode)
            znodesCount+=1
    
    print(allZnodes,"AllZnodes List")
    print(children,"actual znodes")
    try:
        zk.get(allZnodes[0],watch=foo)
    except Exception as e:
        print(e,'error in @childernwacth')
    print(znodesCount,"ZnodesCount")
    # a=[1,2,3,4,5]
    # actu=[1,2,5]
    if(znodesCount>len(children)):
        znodesDeleted=abs(len(children)-znodesCount)
        print(str(znodesDeleted)+" Znodes deleted:( \n")
        znodesCount=len(children)
        temp=[]
        if not scalingDown:
            if c<1: #bcz this watch was getting called twice so to handle that
                
                # print("Creating slave from childrenwatch")
                # createSlaves(znodesDeleted)
                c+=1
            else:
                c+=1
        # scalingDown=False
        for znode in allZnodes.keys():
            if(znode not in children):
                temp.append(znode)
        for znode in temp:
            allZnodes.pop(znode)

    print(allZnodes,"AllZnodes List after ")
    print(children,"actual znodes")

class write_class(object):
	
	def __init__(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
		self.channel = self.connection.channel()
		#self.callback_queue='response_queue'
		#self.channel.queue_declare(queue=self.callback_queue,exclusive=True)
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
		print(n,"input content")
		print(self.corr_id,"generated id")
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
		print("inside on_response",body)
		print(props.correlation_id,"resturned id")
		if self.corr_id == props.correlation_id:
			self.read_response = json.loads(body)
	
	def read_call(self, n):
		self.read_response = None
		self.corr_id = str(uuid.uuid4())
		print(n,"input content_read_call")
		print(self.corr_id,"generated id")
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
	#res=jsonify(response)
	#print(res.json(),"djflsdkfj")
	return jsonify(write_response)

@app.route("/api/v1/db/read",methods=["POST"])
def read_database():
	fun_for_count()
	#lock.acquire()
	file=open("read_count.txt","r")
	count=file.readline()
	print(count,"aaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	count2=file.readline()
	call_deamon=int(count2)
	print(call_deamon,"calling deamon for the first time")
	file.close()
	#lock.release()
	if(call_deamon==0):
		threading.Timer(WAIT_SECONDS,deamon_call).start()
	read_content=request.get_json()
	print(read_content,"content recieved")	
	read_obj=read_class()
	read_response=read_obj.read_call(read_content)
	print(read_response,"response of list of users")
	return jsonify(read_response)


@app.route("/api/v1/worker/list",methods=["GET"])
def worker_list():
	container_list=[]
	x=client.containers.list()
	print(x,"containers")
	for i in x:
		if(i.name=="orchestrator" or i.name=="rabbitmq" or i.name=="cc_final_test_zoo_1"):
			print(i.id,i.name)
		else:
			print(i.name,"4445445455")
			container_list.append(i.id)
	pid_list=[]
	c=docker.APIClient()
	for i in container_list:
		stat=c.inspect_container(i)
		print(type(stat['Args'][1]))
		pid_list.append(stat['State']['Pid'])
	pid_list.sort()
	print(pid_list,"sorted list")
	return json.dumps(pid_list),200

@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
	global spawn_new
	try:
		reason=request.get_json()["reason"]
		if(reason=="scale_in"):
			spawn_new=0
	except:
		spawn_new=2
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
		#print(stat\n)
		PID=(stat['State']['Pid'])
		slave_pid.append(PID)
		mapping[PID]=i
	slave_pid.sort()
	print(slave_pid,"when  only one container is present")
	print(mapping,"mapping dictionary")
	if(len(slave_pid)==0):
		print("no containers to kill")
	largest_pid=slave_pid[len(slave_pid)-1]
	to_be_killed=mapping[largest_pid]
	for j in x:
		if(j.id==to_be_killed):
			j.stop()
			j.remove()
	#if(len(slave_pid)==1):
	#	client.containers.run("slave_image",command="python slave.py",network="cc_final_test_network",detach=True)
	return json.dumps([largest_pid]),200

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
