#base image 
FROM python
#copying all the the data to todo directory
ADD . /todo
#working directory
WORKDIR /todo
#standard output is displayed on the terminal
ENV PYTHONUNBUFFERED=1
ENV TEAM_NAME "CC_1537_1602_1654_1667" 
#install required libraries and bindings
RUN pip install -r requirements.txt
#to create a unique znode 
ENV workerUniqueId=1
#command to run once the container is started
CMD ["python","zk_orch.py"]
