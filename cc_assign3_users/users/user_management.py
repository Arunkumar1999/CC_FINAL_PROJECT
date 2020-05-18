#importing required libraries
import sqlite3
from flask import Flask, render_template,jsonify,request,abort,Response
import requests
import json
import csv
from datetime import datetime

app=Flask(__name__)

cursor = sqlite3.connect("rideshare.db")
file=open("text.txt","w")
file.write("0")
file.close()
def fun_for_count():

	try:
		file=open("text.txt","r")
		#print(file.readline()[0])
		e=file.readline()
		q=int(e)+1
		print(q,"hjkl")
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


cursor.execute("""
        CREATE TABLE IF NOT EXISTS users(
          name varchar(20) primary key,
  		  pass varchar(20)
        );
    """)

cursor.commit()

def fun(passw):
	if(len(passw)!=40):
		return 0
	for i in passw:
		if(not i.isdigit() and not (i.isalpha() and ord('a')<=ord(i) and ord('f')>=ord(i)) ):
			return 0
	return 1


@app.route("/",methods=["GET"])
def hello():
      return "<h1>Hello users</h1>"

@app.route("/api/v1/db/read",methods=["POST"])
def read_database():
	cursor = sqlite3.connect("rideshare.db")
	resp_dict={}
	val=request.get_json()["insert"]
	table=request.get_json()["table"]
	column=request.get_json()["column"]
	where_check_cond=request.get_json()["where"]
	if(len(where_check_cond)>0):
		check_string=""
		for i in range(len(where_check_cond)-1):
			check_string+=where_check_cond[i]+" = "+"'"+val[i]+"'"+" AND "
		check_string+=where_check_cond[len(where_check_cond)-1]+" = "+"'"+val[len(where_check_cond)-1]+"'"
		##print(check_string,"aaaaaaaaaaaaa")
				

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
	
	##print(sql)
	resp=cursor.execute(sql)
	#print(resp)
	resp_check=resp.fetchall()
	print(len(resp_check),"length of resp_check")
	if(len(resp_check) == 0):
		resp_dict["response"]=0
		print("resonse when no users exists")
		return json.dumps(resp_dict)
	else:
		
		#print(resp_check)
		#print(list(resp_check[0]))
		#print(len(resp_check),"count of all rows")
		resp_dict["count"]=resp_check[0]
		for i in range(len(resp_check)):
			for j in range(len(column)):
				resp_dict.setdefault(column[j],[]).append(list(resp_check[i])[j])
		#print(resp_dict,"hii i am dict")
		#print("user does exists from read_Db")
		resp_dict["response"]=1
		return json.dumps(resp_dict)

@app.route("/api/v1/db/write",methods=["POST"])
def to_database():
	
	indicate=request.get_json().get("indicate")
	#print("indicate:", indicate)
	try :
		cursor = sqlite3.connect("rideshare.db")
		cursor.execute("PRAGMA FOREIGN_KEYS=on")
		cursor.commit()
	except Exception as e:
		#print("Database connect error:",e)
		pass
	if(indicate=="0"):
		val=request.get_json().get("insert")
		table=request.get_json().get("table")
		column=request.get_json().get("column")
		#print("val:",val)
		#print("table",table)
		#print("column:", column)
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
			#print("query:",sql)
			cursor.execute(sql,val)

			cursor.commit()
			sql="select * from "+table
			et=cursor.execute(sql)
			rows = et.fetchall()

			sql="select * from users"
			et=cursor.execute(sql)
			rows = et.fetchall()
			return jsonify(1)
		except Exception as e:
			#print(e)
			sql="select * from "+table
			et=cursor.execute(sql)
			rows = et.fetchall()
			#for row in rows:
				#print(row,"we")
			return jsonify(0)
		return jsonify(1)
	elif(indicate=='1'):
		table=request.get_json()["table"]
		delete=request.get_json()["delete"]
		column=request.get_json()["column"]
		#print("table",table)
		#print("delete:",delete)
		try:
			#print("asdf")
			sql="select * from "+table+" WHERE "+column+"=(?)"
			#print("query",sql)
			et=cursor.execute(sql,(delete,))
			if(not et.fetchone()):
				#print("fs")
				return jsonify(0)
			
			sql = "DELETE from "+table+" WHERE "+column+"=(?)"
			#print(table,column,delete)
			#print(sql)
			et=cursor.execute(sql,(delete,))
			#print(et.fetchall())
			cursor.commit()
		except Exception as e:
			#print(e)
			#print("rt")
			return jsonify(0)
		return jsonify(1)
	elif(indicate=='3'):
		try:
			et=cursor.execute("DELETE FROM users")
			cursor.commit()
		except Exception as e:
			return jsonify(0)
		return jsonify(1)



	else:
		return jsonify(0)



@app.route("/api/v1/users",methods=["PUT"])
def add():
	fun_for_count()
	if(request.method!="PUT"):
		abort(405,"method not allowed")

	name=request.get_json()["username"]
	passw=request.get_json()["password"]
	#print("name:", name)
	#print("pass:", passw)
	print("hii from put users")
	d=[name,passw]
	if(fun(passw)==0):
		abort(400,"password is not correct")
	
	res=requests.post("http://18.234.100.70:80/api/v1/db/write",json={"insert":d,"column":["name","pass"],"table":"users","indicate":"0"})	

		
	if(res.json()==0):
			abort(400,"user already exists")	
	

	return Response("success",status=201,mimetype='application/json')


@app.route("/api/v1/users/<name>",methods=["DELETE"])
def remove(name):
	fun_for_count()
	if(request.method!="DELETE"):
		abort(405,"method not allowed")
	res=requests.post("http://18.234.100.70:80/api/v1/db/write",json={"table":"users","delete":name,"column":"name","indicate":"1"})
	#print(res.json())
	if(res.json()==0):
		abort(400,"user does not  exists")
	elif(res.json()==1):
		return json.dumps({'success':"user has been deleted successfully"}), 200, {'ContentType':'application/json'}

@app.route("/api/v1/users",methods=["GET"])
def list_users():
	fun_for_count()
	array_of_users=[]
	if(request.method!="GET"):
		abort(405,"method not allowed")
	
	src_dest_check=requests.post("http://18.234.100.70:80/api/v1/db/read",json={"insert":"","column":["name"],"table":"users","where":[]})
	if(src_dest_check.json().get("response")==0):
		print("empty list no users from list users api")	
		return json.dumps([]),200, {'ContentType':'application/json'}
	timestamp1=src_dest_check.json().get("name")
    # print(timestamp1,"kkkkkkk")
	for i in range(len(timestamp1)):
		array_of_users.append(src_dest_check.json().get("name")[i])	

	# print(type(src_dest_check.json().get("name")[0]),"hhhhhhhh")	
	return json.dumps(array_of_users),200, {'ContentType':'application/json'}

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
	print("couting the number of http requests")
	return json.dumps([int(e)]),200, {'ContentType':'application/json'}

@app.route("/api/v1/_count",methods=["DELETE"])
def clear_http_request():
	if(request.method!="DELETE"):
		abort(405,"method not allowed")
	
	file=open("text.txt","w")
	file.write("0")		

	file.close()
	return json.dumps({'success':"cleared successfully"}), 200, {'ContentType':'application/json'}
	

if __name__ == '__main__':
	app.debug=True
	app.run(host='0.0.0.0',port=80)
