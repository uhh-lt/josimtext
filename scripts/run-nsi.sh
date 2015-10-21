#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ] ; then
    echo "Run noun sense induction"
    echo "parameters: <corpus-directory> <output-directory>"
    exit
fi


# Meta-parameters
param_w=10000 # words per feature
param_s=0.0
param_t_wf=2
param_t_w=10
param_t_f=2
param_sig=LMI
param_r=6
param_p=10000 # features per word
param_l=200 # number of nearest neighbors
maxlen=110
all_words=false
semantify=true

# Process input params
calc_features=true
calc_sims=true
corpus=$1
output=$2
features="$output/features-mln$maxlen-alw$all_words-sem$semantify" 
wordsim="$output/wordsim-fpw$param_p-wpf$param_w-sig$param_sig-nnn$param_l"
wordFeatureCountsFile=$features/DepWF-* 
wordCountsFile=$features/W-* 
featureCountsFile=$features/DepF-* 

echo "Corpus: $corpus"
echo "Output: $output"
echo "Features: $features"
echo "WordSim: $wordsim"


# Generate word-feature matrix
if $calc_features; then  
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
        -Dholing.all_words=$all_words \
        -Dholing.dependencies.semantify=$semantify \
        -Dholing.sentences.maxlength=$maxlen \
        $corpus \
        $features 
fi 

# Calculate word similarities
if $calc_sims; then  
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
        $wordsim \
        $param_w \
        $param_s \
        $param_t_wf \
        $param_t_w \
        $param_t_f \
        $param_sig \
        $param_r \
        $param_p \
        $param_l
fi
