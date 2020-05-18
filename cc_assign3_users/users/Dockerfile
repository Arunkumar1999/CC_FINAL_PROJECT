FROM python                                                                                                                                           
ADD . /todo
WORKDIR /todo
ENV PYTHONUNBUFFERED=1
RUN pip install -r requirements.txt
CMD [ "user_management.py" ]
ENV TEAM_NAME "CC_1537_1602_1654_1667" 
EXPOSE 80
