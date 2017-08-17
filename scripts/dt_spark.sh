#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Compute a distributional thesaurus (DT) from an input corpus, a CoNLL file, or a term-context CSV file."
    echo "parameters: <input-directory> <output-directory> <config.sh>"
    exit
fi

# System parameters
source $3

# Term similarity
WordsPerFeature=1000 # 100 1000 10000
FeaturesPerWord=2000 # 100 1000 10000
MinWordFreq=5
MinFeatureFreq=5
MinWordFeatureFreq=2
Significance=LMI
MinFeatureSignif=0.0
NearestNeighboursNum=200

# Process input params
input=$1
output=$2
wordFeatureCountsFile=$input/WF.*
wordCountsFile=$input/W.*
featureCountsFile=$input/F.*
wordsim="${output}/sign-${Significance}_wpf-${WordsPerFeature}_fpw-${FeaturesPerWord}_minw-${MinWordFreq}_minf-${MinFeatureFreq}_minwf-${MinWordFeatureFreq}_minsign-${MinFeatureSignif}_nnn-${NearestNeighboursNum}"

echo "Input: $input"
echo "Output: $wordsim"
echo "To start press any key, to stop press Ctrl+C"
read -n 2

$spark \
    --class=dt.WordSimFromCounts \
    --master=$master \
    --queue=$queue \
    --num-executors $num_executors \
    --driver-memory ${spark_gb}g \
    --executor-memory ${spark_gb}g \
    $bin_spark \
    $wordCountsFile \
    $featureCountsFile \
    $wordFeatureCountsFile \
    $wordsim \
    $WordsPerFeature \
    $FeaturesPerWord \
    $MinWordFreq \
    $MinFeatureFreq \
    $MinWordFeatureFreq \
    $MinFeatureSignif \
    $Significance \
    $NearestNeighboursNum