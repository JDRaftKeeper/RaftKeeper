#!/bin/bash

binDir=`dirname $0`
libDir=$binDir/../lib

jars=(`ls $libDir`)

classpath='.'
for j in ${jars[*]}
do
    classpath=$classpath:$libDir/$j
done

#for i in 5 10 15 20 25 30 35 40 45 50 60 70 80 90 100 110 120 130 140 150 160 170 180 190 200
#do
#        java -cp $classpath com.jd.raft.benchmark.Main "zookeeper" "10.199.141.13:2181,10.199.140.251:2181,10.199.140.250:2181" ${i} 256 600 "create"
#done
java -cp $classpath com.jd.raft.benchmark.Main "raft" "NODE1:5102,NODE2:5102,NODE3:5102" 100 256 3 "create"
java -cp $classpath com.jd.raft.benchmark.Main "raft" "NODE1:5102,NODE2:5102,NODE3:5102" 100 256 3 "mix"
