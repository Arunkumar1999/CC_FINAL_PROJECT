import sqlite3
import csv
import pika
import time
import json
import sys

time.sleep(10)
cursor = sqlite3.connect("rideshare.db")
cursor.execute("""
        CREATE TABLE IF NOT EXISTS users(
          name varchar(20) primary key,
  		  pass varchar(20)
        );
    """)

cursor.commit()

cursor.execute("""
        CREATE TABLE IF NOT EXISTS rideusers(
         id int not null,
  		 name varchar(20),
		primary key(id,name)
        );
    """)

cursor.execute("""
    CREATE TABLE IF NOT EXISTS place(
      id int primary key,
	  name varchar(20)
    );
""")

with open('AreaNameEnum.csv') as File:  
	reader = csv.reader(File)

	i=0
	for row in reader:
		if(i):
			try:
				d=[row[0],row[1]]
				sql="insert into place values (?,?)"
				
				cursor.execute(sql,d)
			except:
				continue	
		i=1	

cursor.commit()

cursor.execute("""
        CREATE TABLE IF NOT EXISTS rides(
          rideid integer  primary key AUTOINCREMENT,
          name varchar(20) not null ,
  		  timest DATETIME not null,
  		  source varchar(30) not null,
  		  desti varchar(30) not null,
		  FOREIGN KEY (name) REFERENCES users(name) ON DELETE CASCADE
        );
""")

cursor.commit()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
master=int(sys.argv[1])


def synch_to_all(ch, method, properties, body):
	data=json.loads(body)
	print(data,"sync to all")
	if(data["indicate"]==0):
		print(data["sql"],data["val"])
		cursor.execute(data["sql"],data["val"])
		cursor.commit()
	elif(data["indicate"]==1):
		print(data["sql"],data["val"])
		cursor.execute(data["sql"],(data["val"],))
		cursor.commit()
	elif(data["indicate"]==3):
		cursor.execute("DELETE FROM users")
		cursor.execute("DELETE FROM rides")
		cursor.execute("DELETE FROM rideusers")
		cursor.commit()

def copy_db_initial(channel):
    try:
        declareStatus = channel.queue_declare(queue="persistent_queue", durable=True)
        print(declareStatus)
    except Exception as e:
        print(e,"exception")
        print("\n Failed to declare a copyDb queue for syncing whole db in slave\n")
    try:
        noOfMsg=declareStatus.method.message_count
        print("No of msg in the copyDb queue\n",noOfMsg)
        while(noOfMsg>0):
            messageRes = channel.basic_get(queue='persistent_queue',auto_ack=False)
            print(messageRes,"\nMessage got from queue")
            synch_to_all(messageRes[2])
            noOfMsg=noOfMsg-1
    except Exception as e:
        print(e,"exception")
        print("\n Failed to read all messages in the copyDb queue while syncing whole db in slave\n")
    print("Slave consistent now with master!")
    channel.close()



def read_database(ch,method,props,body):
	cursor = sqlite3.connect("rideshare.db")
	print(body,"inside read database")
	resp_dict={}
	data = json.loads(body)
	val=data["insert"]
	print(val,"aaaaaaa")
	table=data["table"]
	column=data["column"]
	where_check_cond=data["where"]
	if(len(where_check_cond)>0):
		check_string=""
		for i in range(len(where_check_cond)-1):
			check_string+=where_check_cond[i]+" = "+"'"+val[i]+"'"+" AND "
		check_string+=where_check_cond[len(where_check_cond)-1]+" = "+"'"+val[len(where_check_cond)-1]+"'"
		print(check_string,"where condition")
				

	r=""
	s=""
	e=len(column)-1
	for i in range(e):
		r+=column[i]+","
		s+="?,"
	r+=column[e]
	s+="?"
	for i in range(len(val)):
		val[i]=val[i].encode("utf8")

	if(len(where_check_cond)>0):
		sql="select "+r+" from "+table+" where "+check_string+";"
	else:
		sql="select "+r+" from "+table+";"
		print(sql,"aaaaaa")
	
	resp=cursor.execute(sql)
	cursor.commit()
	resp_check=resp.fetchall()
	print(len(resp_check),"length of resp_check")
	if(len(resp_check) == 0):
		resp_dict["response"]=0
		print("resonse when no users exists")
	else:
		resp_dict["count"]=resp_check[0]
		for i in range(len(resp_check)):
			for j in range(len(column)):
				resp_dict.setdefault(column[j],[]).append(list(resp_check[i])[j])
		resp_dict["response"]=1

	ch.basic_publish(exchange='', 
		routing_key=props.reply_to, 
    		properties=pika.BasicProperties(correlation_id=props.correlation_id),
		body=json.dumps(resp_dict))
	print(" [x] Sent  ",resp_dict)
	ch.basic_ack(delivery_tag=method.delivery_tag)


def write_database(ch,method,props,body):
	
	data = json.loads(body)
	indicate=data["indicate"]
	try :
		cursor = sqlite3.connect("rideshare.db")
		cursor.execute("PRAGMA FOREIGN_KEYS=on")
		cursor.commit()
	except Exception as e:
		pass
	if(indicate=="0"):
		return_var=None
		print("inside 0 segment")
		val=data["insert"]
		print(val,"aaaaaaa")
		table=data["table"]
		column=data["column"]
		print(table,column)
		r=""
		s=""
		e=len(column)-1
		for i in range(e):	
			r+=column[i]+","
			s+="?,"
		r+=column[e]
		s+="?"
		for i in range(len(val)):
			val[i]=val[i]

		try:

			sql="insert into "+table+" ("+r+")"+" values ("+s+")"
			print(sql,"insert statements")
			cursor.execute(sql,val)

			cursor.commit()
			sql_values=""
			print("rrrrrr")
			for i in range(len(val)-1):
				print(sql_values,"sql_values")
				print(val[i],"gggg")
				sql_values=str(val[i])+","
				print(sql,"kkk")
			sql_values+=str(val[len(val)-1])
			print(sql_values,"afftererrr")
			print("after valllllll")
			sql1="insert into "+table+" ("+r+")"+" values ("+sql_values+")"
			print(sql1,"sql statements")
			sql2={"sql":sql,"val":val,"indicate":0}
			channel.exchange_declare(exchange='logs', exchange_type='fanout')
			channel.basic_publish(exchange='logs', routing_key='', body=(json.dumps(sql2)))
			channel.basic_publish(exchange='',routing_key='persistent_queue',body=(json.dumps(sql2)),properties=pika.BasicProperties(delivery_mode=2,))
			print(type(sql1))
			print(" [x] Sent %r" % sql1)
			print("after fanout")

			return_var=1
		except Exception as e:
			print("errrrrrr")
			return_var=0
	elif(indicate=='1'):
		return_var_delete=None
		print("inside delete section")
		table=data["table"]
		delete=data["delete"]
		column=data["column"]
		try:
			print("asdf")
			sql="select * from "+table+" WHERE "+column+"=(?)"
			print("query",sql)
			et=cursor.execute(sql,(delete,))
			cursor.commit()
			if(not et.fetchone()):
				print("fs")
				print("user doesn't exist")
				return_var_delete=0
			if(return_var_delete!=0):	
				sql = "DELETE from "+table+" WHERE "+column+"=(?)"
				print(sql,delete,"sql statement for deleting")
				et=cursor.execute(sql,(delete,))
				cursor.commit()
				
				sql2={"sql":sql,"val":delete,"indicate":1}
				channel.exchange_declare(exchange='logs', exchange_type='fanout')
				channel.basic_publish(exchange='logs', routing_key='', body=(json.dumps(sql2)))
				channel.basic_publish(exchange='',routing_key='persistent_queue',body=(json.dumps(sql2)),properties=pika.BasicProperties(delivery_mode=2,))
				return_var_delete=1
				
		except Exception as e:
			return_var_delete=0

	elif(indicate=='3'):
		return_var_del_all=None
		try:
			sql="DELETE FROM users"
			cursor.execute("DELETE FROM users")
			cursor.commit()
			cursor.execute("DELETE FROM rides")
			cursor.commit()
			cursor.execute("DELETE FROM rideusers")
			cursor.commit()
			sql2={"indicate":3}
			channel.exchange_declare(exchange='logs', exchange_type='fanout')
			channel.basic_publish(exchange='logs', routing_key='', body=(json.dumps(sql2)))
			channel.basic_publish(exchange='',routing_key='persistent_queue',body=(json.dumps(sql2)),properties=pika.BasicProperties(delivery_mode=2,))
			print("deleted successfully and published the result")
			return_var_del_all=1
		except Exception as e:
			return_var_del_all=0

	else:
		return_var=0
	if(indicate=="1"):
		return_response=return_var_delete
	elif(indicate=="0"):
		return_response=return_var
	elif(indicate=="3"):
		return_response=return_var_del_all
	ch.basic_publish(exchange='', 
		routing_key=props.reply_to, 
    		properties=pika.BasicProperties(correlation_id=props.correlation_id),
		body=json.dumps(return_response))
	print(" [x] Sent  ",return_response)
	ch.basic_ack(delivery_tag=method.delivery_tag)


if(master):
	print("I AM THE MASTER")
	channel.queue_declare(queue='write_queue')
	channel.queue_declare(queue='persistent_queue',durable=True)
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(queue='write_queue', on_message_callback=write_database)

	channel.start_consuming()
elif(not master):
	print("i am the slave")
	channel_for_synch=connection.channel()
	copy_db_initial(channel_for_synch)
	channel.queue_declare(queue='read_queue')
	channel.exchange_declare(exchange='logs', exchange_type='fanout')
	result = channel.queue_declare(queue='', exclusive=True)
	queue_name = result.method.queue
	channel.queue_bind(exchange='logs', queue=queue_name)
	print(' [*] Waiting for logs. To exit press CTRL+C')
	channel.basic_consume(queue=queue_name, on_message_callback=synch_to_all, auto_ack=True)
	channel.basic_qos(prefetch_count=1)
	print("start consuming for read-queue")
	channel.basic_consume(queue='read_queue', on_message_callback=read_database)
	channel.start_consuming()	
