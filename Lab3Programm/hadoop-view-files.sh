#!/bin/bash

export HADOOP_HOME=~/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

echo ""
echo "Hadoop working folder"
hdfs dfs -ls /user/jbisss

echo ""
echo "List of .csv files"
hdfs dfs -ls /user/jbisss/input_csv

echo ""
echo "Top 10 records of first .csv (0.csv) for demonstration"
hdfs dfs -cat /user/jbisss/input_csv/0.csv | head -n 10