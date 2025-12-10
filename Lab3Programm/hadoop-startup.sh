#!/bin/bash

export HADOOP_HOME=~/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

start-dfs.sh

# start-yarn.sh

# check
# hdfs dfs -ls /