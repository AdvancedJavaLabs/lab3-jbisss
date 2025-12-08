#!/bin/bash

REBUILD_FLAG=$1

LOCAL_INPUT_DIR=./input_csv
HDFS_INPUT_DIR=/user/jbisss/input_csv
HDFS_BASE_OUTPUT_DIR=/user/jbisss/output
HDFS_BASE_OUTPUT_SORTED_DIR=/user/jbisss/output_sorted
JAR_FILE=/home/jbisss/IdeaProjects/lab3-jbisss/Lab3Programm/target/Lab3Programm-1.0-SNAPSHOT.jar
FINAL_REPORT=final_report.txt
DRIVER_CLASS=com.itmo.ipkn.Stats

MAVEN_HOME="/home/jbisss/Downloads/ideaIU-2025.2.1/idea-IU-252.25557.131/plugins/maven/lib/maven3"

export HADOOP_HOME=~/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

rm $FINAL_REPORT

if [ "$REBUILD_FLAG" = "0" ]; then
    echo "Rebuilding"
    "$MAVEN_HOME/bin/mvn" clean package || { echo "Rebuilding exited with an error"; exit 1; }
else
    echo "Rebuilding skipped"
fi

[ ! -f "$JAR_FILE" ] && echo "JAR not found" && exit 1

hdfs dfs -rm -r -f ${HDFS_BASE_OUTPUT_DIR}_*
hdfs dfs -rm -r -f ${HDFS_BASE_OUTPUT_SORTED_DIR}_*

hdfs dfs -mkdir -p $HDFS_INPUT_DIR
hdfs dfs -put -f $LOCAL_INPUT_DIR/* $HDFS_INPUT_DIR/

REDUCERS_ARRAY=(1 4 12 36)

for reducers in "${REDUCERS_ARRAY[@]}"; do
    HDFS_OUTPUT_DIR="${HDFS_BASE_OUTPUT_DIR}_reducers_${reducers}"
    HDFS_OUTPUT_SORTED_DIR="${HDFS_BASE_OUTPUT_SORTED_DIR}_reducers_${reducers}"

    echo "Starting with $reducers reducers"
    hadoop jar $JAR_FILE $DRIVER_CLASS $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $HDFS_OUTPUT_SORTED_DIR $reducers

    TEMP_FILE="temp_output.txt"
    hdfs dfs -getmerge $HDFS_OUTPUT_SORTED_DIR $TEMP_FILE

    {
        echo "With $reducers reducers"
        echo ""
        cat $TEMP_FILE
        echo ""
        echo "Reducers=$reducers"
        echo ""
        echo ""
    } >> $FINAL_REPORT

    rm $TEMP_FILE
done

echo "Final report: $FINAL_REPORT"
