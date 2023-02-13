#!/bin/bash

binDir=`dirname $0`
libDir=$binDir/../lib

jars=(`ls $libDir`)

classpath='.'
for j in ${jars[*]}
do
    classpath=$classpath:$libDir/$j
done

java -cp $classpath com.jd.raft.benchmark.Main "zookeeper" "10.199.141.13:8200,10.199.140.251:8200,10.199.140.250:8200" 200 1024 10 "createBase" 15 0
