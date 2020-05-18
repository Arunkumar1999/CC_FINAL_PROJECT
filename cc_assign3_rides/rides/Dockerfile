
FROM python
ADD . /todo
WORKDIR /todo
RUN apt update
RUN pip install -r requirements.txt
CMD [ "ride_management.py" ]
ENV PYTHONUNBUFFERED=1
ENV TEAM_NAME "CC_1537_1602_1654_1667"
EXPOSE 80
