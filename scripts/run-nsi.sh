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
Significance=LMI
WordsPerFeature=1000 
MinFeatureSignif=0.0
MinWordFeatureFreq=2
MinWordFreq=5
MinFeatureFreq=5
SimPrecision=5
FeaturesPerWord=1000 
NearestNeighboursNum=200 

# Process input params
corpus=$1
output=$2
calc_features=$3
calc_sims=$4
queue=$5
features="$output/Features__Holing-${holing_type}_Lemmatize-${lemmatize}_Coocs-${coocs}_MaxLen-${maxlen}_NounsOnly-${nouns_only}_NounNounOnly-${noun_noun_only}_Semantify-${semantify}" 
wordFeatureCountsFile=$features/WF-* 
wordCountsFile=$features/W-* 
featureCountsFile=$features/F-* 
wordsim="$output/Similarities__Significance-${Significance}_WordsPerFeature-${WordsPerFeature}_FeaturesPerWord-${FeaturesPerWord}_MinWordFreq-${MinWordFreq}_MinFeatureFreq-${MinFeatureFreq}_MinWordFeatureFreq-${MinWordFeatureFreq}_MinFeatureSignif-${MinFeatureSignif}_SimPrecision-${SimPrecision}_NearestNeighboursNum-${NearestNeighboursNum}"

echo "Corpus: $corpus"
echo "Output: $output"
echo "Output features: $features"
echo "Output similarities: $wordsim"
echo "Calculate features: $calc_features"
echo "Calculate similarities: $calc_sims"

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
        $WordsPerFeature \
        $MinFeatureSignif \
        $MinWordFeatureFreq \
        $MinWordFreq \
        $MinFeatureFreq \
        $Significance \
        $SimPrecision \
        $FeaturesPerWord \
        $NearestNeighboursNum
fi
