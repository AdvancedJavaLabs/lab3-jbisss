#!/bin/sh
clear

export HADOOP_HOME=~/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

pwd

rm -r ../output
rm -r ../output_sorted

REBUILD_FLAG=$1
if [ "$REBUILD_FLAG" = "0" ]; then
  MAVEN_HOME="/home/jbisss/Downloads/ideaIU-2025.2.1/idea-IU-252.25557.131/plugins/maven/lib/maven3"
  "$MAVEN_HOME/bin/mvn" clean package
fi

hadoop jar /home/jbisss/IdeaProjects/lab3-jbisss/Lab3Programm/target/Lab3Programm-1.0-SNAPSHOT.jar com.itmo.ipkn.Stats