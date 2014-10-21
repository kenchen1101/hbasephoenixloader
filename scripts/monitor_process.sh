#!/bin/bash

echo "Starting Stats Collection at dir : [`pwd`]"

NOW=$(date +"%d-%m-%Y::%H:%M:%S")
memoryStatsFile=./logs/FreeMemoryStats.log
processStatsFile=./logs/ProcessStats.log
processName=6861

echo "#Memory Stats for Messaging Service $NOW" > $memoryStatsFile
echo "#Process Stats for Messaging Service $NOW" > $processStatsFile

while [ 1 ]
do
   CURRENT_TIME=$(date +"%d-%m-%Y::%H:%M:%S")
   echo $CURRENT_TIME >> $memoryStatsFile
   free -m >> $memoryStatsFile
   echo $CURRENT_TIME >> $processStatsFile
   ps -eo pid,pcpu,size,rss,pmem,thcount,args | grep $processName | grep -v grep | awk '{print "PID: " $1 "\t" "CPU: " $2 "\t" "SIZE: " $3 "\t" "RSS: " $4 "\t" "MEMORY: " $5 "\t" "THREAD COUNT:" $6}' >> $processStatsFile
   sleep 5
done