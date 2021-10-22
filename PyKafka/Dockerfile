FROM python:3

MAINTAINER Adeline Chew

WORKDIR /app

ADD m_consumer.py /app

ADD train.csv /app

RUN pip3 install kafka-python

CMD ["python", "./m_consumer.py"]
