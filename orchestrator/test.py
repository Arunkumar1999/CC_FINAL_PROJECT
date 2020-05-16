import docker 
client = docker.from_env()
#client.images.build(path="/home/ubuntu/CC_FINAL_TEST/slave/",tag="aaa")
#client.containers.run("slave_image",command="python slave.py",network="cc_final_test_network",name="slave2")
#print("hiii")
#client = docker.from_env()
#for c in client.containers.list():
#	c.stop()
#	print("khel katam natak band")
for c in client.containers.list(filters={"ancestor":"master_image"}):
	print(c.name,c.id,c.status)

#c = docker.APIClient()
#aa=c.inspect_container("slave")
#print(type(aa['State']['Pid']))
#import docker
#client = docker.from_env()
#client.containers.stop('slave')
