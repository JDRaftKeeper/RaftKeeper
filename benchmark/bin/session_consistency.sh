#!/bin/bash

binDir=`dirname $0`
libDir=$binDir/../lib
confDir=$binDir/../conf

jars=(`ls $libDir`)

classpath='.'
for j in ${jars[*]}
do
    classpath=$classpath:$libDir/$j
done
classpath=$confDir:$classpath

# Test read write consistency in one session
# args: nodes thread_size
# for example : ./benchmark.sh "localhost:2181" 10

nodes=$1
thread_size=$2
java -cp $classpath raftkeeper.SessionReadWriteConsistencyTest $nodes $thread_size