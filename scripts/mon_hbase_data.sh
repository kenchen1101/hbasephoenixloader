#!/bin/bash

echo "Starting Stats Collection at dir : [`pwd`]"

NOW=$(date +"%d-%m-%Y::%H:%M:%S")
memoryStatsFile=./DFSStats.log

echo "#Memory Stats for Messaging Service $NOW" > $memoryStatsFile

while [ 1 ]
do
   CURRENT_TIME=$(date +"%d-%m-%Y::%H:%M:%S")
   echo $CURRENT_TIME >> $memoryStatsFile
   /home/vikuser/current/bigdata-hadoop/hadoop/hadoop/bin/hadoop dfs -du /hbase >> $memoryStatsFile
   sleep 10
done
