#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ] ; then
    echo "Run a job from the Noun Sense Induction project."
    echo "parameters: <hdfs dir with features> <hdfs dir with wordsim>"
    exit
fi

# detailed param information:
# http://maggie.lt.informatik.tu-darmstadt.de/jobimtext/documentation/calculate-a-distributional-thesaurus-dt/

inDir=$1
outDir=$2
wordFeatureCountsFile=$inDir/DepWF-* 
wordCountsFile=$inDir/W-* 
featureCountsFile=$inDir/DepF-* 
param_w=10000 # words per feature
param_s=0.0
param_t_wf=2
param_t_w=10
param_t_f=2
param_sig=LMI
param_r=3
param_p=10000 # features per word
param_l=200 # number of nearest neighbors

bin=/home/panchenko/bin-spark

export HADOOP_CONF_DIR=/etc/hadoop/conf/
export YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn/

/home/panchenko/spark-1.2.1-bin-hadoop2.4/bin/spark-submit \
    --class=WordSimFromCounts \
    --master=yarn-cluster \
    --queue=shortrunning \
    --num-executors 260 \
    --driver-memory 7g \
    --executor-memory 1g \
    $bin/noun-sense-induction_2.10-0.0.1.jar \
    $wordFeatureCountsFile \
    $wordCountsFile \
    $featureCountsFile \
    $outDir \
    $param_w \
    $param_s \
    $param_t_wf \
    $param_t_w \
    $param_t_f \
    $param_sig \
    $param_r \
    $param_p \
    $param_l

