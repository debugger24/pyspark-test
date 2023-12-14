FROM python:3.7.9-alpine3.12

# JRE
RUN apk --update add openjdk8-jre gcc musl-dev bash
ENV JAVA_HOME /usr/

# Hadoop
ENV HADOOP_VERSION 3.3.3
ENV HADOOP_HOME /usr/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
RUN wget "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
    && tar -xzf "hadoop-$HADOOP_VERSION.tar.gz" -C /usr/ \
    && rm "hadoop-$HADOOP_VERSION.tar.gz"

# Spark
ENV SPARK_VERSION 3.3.3
ENV SPARK_PACKAGE spark-$SPARK_VERSION
ENV SPARK_HOME /usr/$SPARK_PACKAGE-bin-without-hadoop
ENV PYSPARK_PYTHON python
ENV PYSPARK_DRIVER_PYTHON python 
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
ENV PATH $PATH:$SPARK_HOME/bin
RUN wget "https://downloads.apache.org/spark/$SPARK_PACKAGE/$SPARK_PACKAGE-bin-without-hadoop.tgz" \
    && tar -xzf "$SPARK_PACKAGE-bin-without-hadoop.tgz" -C /usr/ \
    && rm "$SPARK_PACKAGE-bin-without-hadoop.tgz"

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY src src
COPY tests tests

RUN pytest --testdox
