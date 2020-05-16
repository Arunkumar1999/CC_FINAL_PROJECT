FROM python
ADD . /todo
WORKDIR /todo
ENV PYTHONUNBUFFERED=1 
ENV TEAM_NAME "CC_1537_1602_1654_1667"
RUN pip install docker
RUN pip install -r requirements.txt 
CMD ["python","orchestrator.py"] 

