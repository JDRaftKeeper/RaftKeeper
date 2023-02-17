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

# benchmark
# args: target nodes thread_size payload_size run_duration(second) only_create
# for example : ./session_consistency.sh "localhost:2181" 10 100 20 true

nodes=$1
thread_size=$2
payload_size=$3
run_duration=$4
only_create=$5
java -cp $classpath raftkeeper.Benchmark "zookeeper" $nodes $thread_size $payload_size $run_duration $only_create
