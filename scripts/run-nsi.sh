#!/bin/bash

if [ -z "$1" ] || [ -z "$4" ] ; then
    echo "Run noun sense induction"
    echo "parameters: <corpus-directory> <output-directory> <do-calc-features> <do-calc-sims> <queue>"
    echo "<queue>  shortrunning, longrunning"
    exit
fi

# Meta-paramters of feature extraction
holing_type="dependency"
lemmatize=true
coocs=false
maxlen=110
nouns_only=false
noun_noun_only=false
semantify=true

# Meta-parameters of similarity
param_w=10000 # words per feature
param_s=0.0
param_t_wf=2
param_t_w=10
param_t_f=2
param_sig=LMI
param_r=9
param_p=1000 # features per word
param_l=200 # number of nearest neighbors

# Process input params
corpus=$1
output=$2
calc_features=$3
calc_sims=$4
queue=$5
features="$output/Holing-${holing_type}_Lemmatize-${lemmatize}_Coocs-${coocs}_MaxLen-${maxlen}_NounsOnly-${nouns_only}_NounNounOnly-${noun_noun_only}_Semantify-${semantify}" 
wordsim="$output/wordsim-fpw$param_p-wpf$param_w-sig$param_sig-nnn$param_l"
wordFeatureCountsFile=$features/WF-* 
wordCountsFile=$features/W-* 
featureCountsFile=$features/F-* 

echo "Corpus: $corpus"
echo "Output: $output"
echo "Features: $features"
echo "WordSim: $wordsim"
echo "Calc features: $calc_features"
echo "Calc similarities: $calc_sims"

echo "To start press any key, to stop press Ctrl+C"
read -n 2

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
        -Dmapreduce.job.queuename=$queue\
        -Dmapreduce.map.java.opts=-Xmx4g \
        -Dmapreduce.map.memory.mp=4096 \
        -Dmapreduce.task.io.sort.mb=1028 \
        -Dmapred.max.split.size=1000000 \
        -Dholing.type=$holing_type \
        -Dholing.coocs=$coocs \
        -Dholing.nouns_only=$nouns_only \
        -Dholing.dependencies.semantify=$semantify \
        -Dholing.sentences.maxlength=$maxlen \
        -Dholing.lemmatize=$lemmatize \
        -Dholing.dependencies.noun_noun_dependencies_only=$noun_noun_only \
        $corpus \
        $features 
fi 

# Calculate word similarities
if $calc_sims; then  
    bin=/home/panchenko/bin-spark

    export HADOOP_CONF_DIR=/etc/hadoop/conf/
    export YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn/

    /usr/bin/spark-submit \
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
