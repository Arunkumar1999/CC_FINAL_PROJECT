#base image 
FROM python
#copying all the the data to todo directory
ADD . /todo
#working directory
WORKDIR /todo
#standard output is displayed on the terminal
ENV PYTHONUNBUFFERED=1 
ENV TEAM_NAME "CC_1537_1602_1654_1667"
RUN pip install docker
#install required libraries and bindings
RUN pip install -r requirements.txt 
#command to run once the container is started
CMD ["python","orchestrator.py"] 

