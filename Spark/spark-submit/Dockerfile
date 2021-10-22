FROM adelineyaoy/spark-base

LABEL maintainer="Adeline Chew"

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_JAR_LOCATION /app/application.jar
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_MAIN_CLASS my.main.Application
ENV SPARK_APPLICATION_ARGS ""

COPY submit.sh /
RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.26/mysql-connector-java-8.0.26.jar

RUN mv mysql-connector-java-8.0.26.jar /spark/jars/

CMD ["/bin/bash", "/submit.sh"]
