#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ] ; then
    echo "Run a job from the Noun Sense Induction project."
    echo "parameters: <hdfs path to input corpus> <hdfs path to output directory with features>"
    exit
fi

input=$1  
output=$2 
bin=/home/panchenko/bin-hadoop

jars=`echo $bin/*.jar | tr " " ","`
path=`echo $bin/*.jar | tr " " ":"`

HADOOP_CLASSPATH=$path hadoop \
    de.tudarmstadt.lt.wsi.JoBimExtractAndCount \
    -libjars $jars \
    -Dmapreduce.reduce.failures.maxpercent=10 \
    -Dmapreduce.map.failures.maxpercent=10 \
    -Dmapreduce.job.queuename=shortrunning \
    -Dmapreduce.map.java.opts=-Xmx4g \
    -Dmapreduce.map.memory.mp=4096 \
    -Dmapreduce.task.io.sort.mb=1028 \
    -Dmapred.max.split.size=1000000 \
    -Dholing.dependencies=true \
    -Dholing.coocs=true \
    -Dholing.all_words=false \
    -Dholing.dependencies.semantify=true \
    -Dholing.sentences.maxlength=110 \
    $input \
    $output 

