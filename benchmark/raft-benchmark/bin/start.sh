#!/bin/bash

binDir=`dirname $0`
libDir=$binDir/../lib

jars=(`ls $libDir`)

classpath='.'
for j in ${jars[*]}
do
    classpath=$classpath:$libDir/$j
done

for i in 1 2 3 4 5 6 7 8 9 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100 110 120 130 140 150 160 170 180 190 200
do

  	java -cp $classpath com.jd.raft.benchmark.Main "zookeeper" "10.199.141.13:8200,10.199.140.251:8200,10.199.140.250:8200" ${i} 256 10 "mix"
	  java -cp $classpath com.jd.raft.benchmark.Main "zookeeper" "10.199.141.13:8200,10.199.140.251:8200,10.199.140.250:8200" ${i} 256 10 "create"
	  java -cp $classpath com.jd.raft.benchmark.Main "zookeeper" "10.199.141.13:8200,10.199.140.251:8200,10.199.140.250:8200" ${i} 256 10 "get" 50
done
