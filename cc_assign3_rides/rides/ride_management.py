import sqlite3
from flask import Flask, render_template,jsonify,request,abort,Response
import requests
import json
import csv
from datetime import datetime

app=Flask(__name__)
# cursor = sqlite3.connect("rideshare_of_rides.db")
file=open("text.txt","w")
file.write("0")
file.close()
def fun():

	try:
		file=open("text.txt","r")
		#print(file.readline()[0])
		e=file.readline()
		q=int(e)+1
		print(q,"kkkk")
		file.close()
		file=open("text.txt","w")
		file.write(str(q))		
		#print("as")
		#file.close()
	except:
		file=open("text.txt","w")
		file.write("0")		
		#print("read")
	file.close()

#fun()
# cursor.execute("""
#         CREATE TABLE IF NOT EXISTS rideusers(
#          id int not null,
#   		 name varchar(20),
# 		primary key(id,name)
#         );
#     """)

# cursor.execute("""
#     CREATE TABLE IF NOT EXISTS place(
#       id int primary key,
# 	  name varchar(20)
#     );
# """)

# with open('AreaNameEnum.csv') as File:  
# 	reader = csv.reader(File)

# 	i=0
# 	for row in reader:
# 		if(i):
# 			try:
# 				d=[row[0],row[1]]
# 				sql="insert into place values (?,?)"
				
# 				cursor.execute(sql,d)
# 			except:
# 				continue	
# 		i=1	

# cursor.commit()

# cursor.execute("""
#         CREATE TABLE IF NOT EXISTS rides(
#           rideid integer  primary key AUTOINCREMENT,
#           name varchar(20) not null ,
#   		  timest DATETIME not null,
#   		  source varchar(30) not null,
#   		  desti varchar(30) not null
#         );
# """)

# cursor.commit()

@app.route("/", methods=["GET"])
def hello():
     return "<h1>Hello</h1>"

# @app.route("/api/v1/db/read",methods=["POST"])
# def read_database():
# 	cursor = sqlite3.connect("rideshare_of_rides.db")
# 	resp_dict={}
# 	val=request.get_json()["insert"]
# 	table=request.get_json()["table"]
# 	column=request.get_json()["column"]
# 	where_check_cond=request.get_json()["where"]
# 	if(len(where_check_cond)>0):
# 		check_string=""
# 		for i in range(len(where_check_cond)-1):
# 			check_string+=where_check_cond[i]+" = "+"'"+val[i]+"'"+" AND "
# 		check_string+=where_check_cond[len(where_check_cond)-1]+" = "+"'"+val[len(where_check_cond)-1]+"'"
# 		##print(check_string,"aaaaaaaaaaaaa")
				

# 	r=""
# 	s=""
# 	e=len(column)-1
# 	for i in range(e):
# 		r+=column[i]+","
# 		s+="?,"
# 	r+=column[e]
# 	s+="?"
# 	for i in range(len(val)):
# 		val[i]=val[i].encode("utf8")

# 	if(len(where_check_cond)>0):
# 		sql="select "+r+" from "+table+" where "+check_string+";"
# 	else:
# 		sql="select "+r+" from "+table+";"
# 		print(sql,"ghgghhghghghghghhg")
	
	
# 	resp=cursor.execute(sql)
# 	resp_check=resp.fetchall()
# 	if(len(resp_check) == 0):
# 		resp_dict["response"]=0
# 		return json.dumps(resp_dict)
# 	else:
		
# 		#print(resp_check)
# 		#print(list(resp_check[0]))
# 		#print(len(resp_check),"count of all rows")
# 		resp_dict["count"]=resp_check[0]
# 		for i in range(len(resp_check)):
# 			for j in range(len(column)):
# 				resp_dict.setdefault(column[j],[]).append(list(resp_check[i])[j])
# 		#print(resp_dict,"hii i am dict")
# 		#print("user does exists from read_Db")
# 		resp_dict["response"]=1
# 		return json.dumps(resp_dict)

# @app.route("/api/v1/db/write",methods=["POST"])
# def to_database():
	
# 	indicate=request.get_json().get("indicate")
# 	#print("indicate:", indicate)
# 	try :
# 		cursor = sqlite3.connect("rideshare_of_rides.db")
# 		cursor.execute("PRAGMA FOREIGN_KEYS=on")
# 		cursor.commit()
# 	except Exception as e:
# 		#print("Database connect error:",e)
# 		pass
# 	if(indicate=="0"):
# 		val=request.get_json().get("insert")
# 		table=request.get_json().get("table")
# 		column=request.get_json().get("column")
# 		#print("val:",val)
# 		#print("table",table)
# 		#print("column:", column)
# 		r=""
# 		s=""
# 		e=len(column)-1
# 		for i in range(e):	
# 			r+=column[i]+","
# 			s+="?,"
# 		r+=column[e]
# 		s+="?"
# 		for i in range(len(val)):
# 			val[i]=val[i]

# 		try:

# 			sql="insert into "+table+" ("+r+")"+" values ("+s+")"
# 			#print("query:",sql)
# 			cursor.execute(sql,val)

# 			cursor.commit()
# 			# sql="select * from "+table
# 			# et=cursor.execute(sql)
# 			# rows = et.fetchall()

# 			# sql="select * from users"
# 			# et=cursor.execute(sql)
# 			# rows = et.fetchall()
# 			#return jsonify(1)
# 		except Exception as e:
# 			#print(e)
# 			sql="select * from "+table
# 			et=cursor.execute(sql)
# 			rows = et.fetchall()
# 			#for row in rows:
# 				#print(row,"we")
# 			return jsonify(0)
# 		return jsonify(1)
# 	elif(indicate=='1'):
# 		table=request.get_json()["table"]
# 		delete=request.get_json()["delete"]
# 		column=request.get_json()["column"]
# 		#print("table",table)
# 		#print("delete:",delete)
# 		try:
# 			#print("asdf")
# 			sql="select * from "+table+" WHERE "+column+"=(?)"
# 			#print("query",sql)
# 			et=cursor.execute(sql,(delete,))
# 			if(not et.fetchone()):
# 				#print("fs")
# 				return jsonify(0)
			
# 			sql = "DELETE from "+table+" WHERE "+column+"=(?)"
# 			#print(table,column,delete)
# 			#print(sql)
# 			et=cursor.execute(sql,(delete,))
# 			#print(et.fetchall())
# 			cursor.commit()
# 		except Exception as e:
# 			#print(e)
# 			#print("rt")
# 			return jsonify(0)
# 		return jsonify(1)
# 	elif(indicate=='3'):
# 		try:
# 			et=cursor.execute("DELETE FROM rides")
# 			cursor.execute("DELETE FROM rideusers")
# 			cursor.commit()
# 		except Exception as e:
# 			return jsonify(0)
# 		return jsonify(1)



# 	else:
# 		return jsonify(0)


@app.route("/api/v1/rides",methods=["POST"])
def insert_rider():
	fun()
	if(request.method!="POST"):
		abort(405,"method not allowed")
	global rides_id
	
	name=request.get_json()["created_by"]
	timestamp=request.get_json()["timestamp"]
	source=request.get_json()["source"]
	destination=request.get_json()["destination"]
    
	d=[name,timestamp,source,destination]
	#print(type(timestamp.encode("utf8")))
	#print(timestamp.encode("utf8"))
	try:
		time_object=datetime.strptime(timestamp,'%d-%m-%Y:%S-%M-%H')
	except:
		#print("hii")
		abort(400,"invalid input")

	read_res=requests.get("http://CC-Project-1005740733.us-east-1.elb.amazonaws.com:80/api/v1/users",headers={"Origin":"18.212.35.29"})
	if(read_res.json()==0):
		abort(400,"no users to fetch")
	if(name not in read_res.json()):
		abort(400,"user doesn't exist")
	
	res=requests.post("http://18.234.100.70:80/api/v1/db/write",json={"insert":d,"column":["name","timest","source","desti"],"table":"rides","indicate":"0"})	
	if(res.json()==0):
		abort(400,"user already exists")

	return Response("success",status=201,mimetype='application/json')


@app.route("/api/v1/rides/<rideId>",methods=["DELETE"])
def delete_rideId(rideId):
	fun()
	if(request.method!="DELETE"):
		abort(405,"method not allowed")
	
	res=requests.post("http://18.234.100.70:80/api/v1/db/write",json={"table":"rides","delete":rideId,"column":"rideid","indicate":"1"})
	if(res.json()==0):
		abort(400,"rideId does not  exists")
	elif(res.json()==1):
		return json.dumps({'success':"deleted successfully"}), 200, {'ContentType':'application/json'}

@app.route("/api/v1/rides/<rideId>",methods=["POST"])
def join_ride(rideId):
	fun()
	if(request.method!="POST"):
		abort(405,"method not allowed")
	
	name=request.get_json()["username"]
	d=[name,rideId]
	#read_res=requests.post("http://172.17.0.1:8000/api/v1/db/read",json={"insert":d,"column":["name","pass"],"table":"users","where":["name"]})
	read_res=requests.get("http://CC-Project-1507675041.us-east-1.elb.amazonaws.com:80/api/v1/users")

	if(read_res.json()==0):
		abort(400,"no user to fetch")
	if(name not in read_res.json()):
		abort(400,"user doesn't exist")
	d=[rideId,name]
	rideid_check=requests.post("http://18.234.100.70:80/api/v1/db/read",json={"insert":d,"column":["rideid","name","source","desti"],"table":"rides","where":["rideid"]})
	if(rideid_check.json().get("response")==0):
		abort(400,"ride id doesn't exists")
	
	res=requests.post("http://18.234.100.70:80/api/v1/db/write",json={"insert":d,"column":["id","name"],"table":"rideusers","indicate":"0"})
	if(res.json()==0):
		abort(400,"rideId does not  exists")
	elif(res.json()==1):
		return json.dumps({'success':"joined successfully"}), 200, {'ContentType':'application/json'}


@app.route("/api/v1/rides/<rideId>",methods=["GET"])
def ride_details(rideId):
	fun()
	if(request.method!="GET"):
		abort(405,"method not allowed")
	users=""
	
	d=[rideId]
	user_list=[]
	rideid_check=requests.post("http://18.234.100.70:80/api/v1/db/read",json={"insert":d,"column":["rideid","name","source","desti","timest"],"table":"rides","where":["rideid"]})
	if(rideid_check.json().get("response")==0):
		abort(400,"rideId does not  exists")
	elif(rideid_check.json().get("response")==1):
		
		joined_users_check=requests.post("http://18.234.100.70:80/api/v1/db/read",json={"insert":d,"column":["id","name"],"table":"rideusers","where":["id"]})


		return json.dumps({"rideId":rideid_check.json().get("rideid"),
							"created_by":rideid_check.json().get("name")[0],
							"users":joined_users_check.json().get("name"),
							"timestamp":rideid_check.json().get("timest"), 
							"source":rideid_check.json().get("source"),
							"destination":rideid_check.json().get("desti")}),200, {'ContentType':'application/json'}

@app.route("/api/v1/rides",methods=["GET"])
def upcoming_rides():
	fun()
	if(request.method!="GET"):
		abort(405,"method not allowed")
	array_of_rides=[]
	
	dateTimeObj = datetime.now()
	string=""
	string+=str(dateTimeObj.day)+"-"+str(dateTimeObj.month)+"-"+str(dateTimeObj.year)+":"+str(dateTimeObj.second)+"-"+str(dateTimeObj.minute)+"-"+str(dateTimeObj.hour)
	source=request.args.get('source')
	destination=request.args.get('destination')
	d=[source,destination]				
	src_dest_check=requests.post("http://18.234.100.70:80/api/v1/db/read",json={"insert":d,"column":["rideid","name","source","desti","timest"],"table":"rides","where":['source','desti']})			
	if(src_dest_check.json().get("response")==0):
		return "no content to send",204,{"ContentType":"application/json"}
	
	str_timestmp=src_dest_check.json().get("timest")
	timestamp1=src_dest_check.json().get("timest")
	for i in range(len(timestamp1)):
		t1 = datetime.strptime(timestamp1[i], "%d-%m-%Y:%S-%M-%H")
		t2 = datetime.strptime(string, "%d-%m-%Y:%S-%M-%H")
		if(t1>t2):
			array_of_rides.append({"rideId":src_dest_check.json().get("rideid")[i],
								"username":src_dest_check.json().get("name")[i],
								"timestamp":src_dest_check.json().get("timest")[i]})

	return json.dumps(array_of_rides),200,{"ContentType":"application/json"}


@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
	if(request.method!="POST"):
		abort(405,"method not allowed")
	
	res=requests.post("http://18.234.100.70:80/api/v1/db/write",json={"indicate":"3"})	
	if(res.json()==0):
		abort(400,"failed to clear")
	elif(res.json()==1):
		return json.dumps({'success':"cleared successfully"}), 200, {'ContentType':'application/json'}
	
@app.route("/api/v1/_count",methods=["GET"])
def get_http_request():
	if(request.method!="GET"):
		abort(405,"method not allowed")
	try:
		file=open("text.txt","r")
		e=file.readline()
		file.close()
	except:
		return json.dumps([0]),200, {'ContentType':'application/json'}	
	return json.dumps([int(e)]),200, {'ContentType':'application/json'}

@app.route("/api/v1/_count",methods=["DELETE"])
def clear_http_request():
	if(request.method!="DELETE"):
		abort(405,"method not allowed")
	
	file=open("text.txt","w")
	file.write("0")		

	file.close()
	return json.dumps({'success':"cleared successfully"}), 200, {'ContentType':'application/json'}

@app.route("/api/v1/rides/count",methods=["GET"])
def ride_count():
	fun()
	if(request.method!="GET"):
		abort(405,"method not allowed")
	users=""
	
	#d=[rideId]
	user_list=[]
	rideid_check=requests.post("http://18.234.100.70:80/api/v1/db/read",json={"insert":[],"column":["count(*) as count"],"table":"rides","where":[]})
	
	if(rideid_check.json().get("response")==1):
		print(rideid_check.json().get("count"),"ssss")
		return json.dumps(rideid_check.json().get("count")),200, {'ContentType':'application/json'}



if __name__ == '__main__':
	app.debug=True
	app.run(host='0.0.0.0',port=80)
