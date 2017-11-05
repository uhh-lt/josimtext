#!/usr/bin/env bash

set -o nounset # Error on referencing undefined variables, shorthand: set -n
set -o errexit # Abort on error, shorthand: set -e

if [ $# -lt 4 ]; then
    echo "Compute a DT from different input formats: [conll, corpus, termcontext]"
    echo "parameters: <format> <input-directory> <output-directory> <config.sh>"
    exit
fi

# System parameters
source $4

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

format=${1}
input=${2}
output=${3}
wordsim="${output}/sign-${Significance}_wpf-${WordsPerFeature}_fpw-${FeaturesPerWord}_minw-${MinWordFreq}_minf-${MinFeatureFreq}_minwf-${MinWordFeatureFreq}_minsign-${MinFeatureSignif}_nnn-${NearestNeighboursNum}"

echo "Format: $format"
echo "Input: $input"
echo "Output: $output"
echo "To start press any key, to stop press Ctrl+C"
read -n 2

if [ "$format" = "conll" ];
 then
    $spark \
    --class=de.uhh.lt.jst.dt.CoNLL2DepTermContext \
    --master=$master \
    --queue=$queue \
    --num-executors $num_executors \
    --driver-memory ${spark_gb}g \
    --executor-memory ${spark_gb}g \
    $bin_spark \
    $input \
    "$output/term-context"

    input="$output/term-context"
    format="termcontext"
elif [ "$format" = "corpus" ]
then
    $spark \
    --class=de.uhh.lt.jst.dt.Text2TrigramTermContext \
    --master=$master \
    --queue=$queue \
    --num-executors $num_executors \
    --driver-memory ${spark_gb}g \
    --executor-memory ${spark_gb}g \
    $bin_spark \
    $input \
    "$output/term-context"

    input="$output/term-context"
    format="termcontext"
fi

if [ "$format" = "termcontext" ]
then
    $spark \
    --class=de.uhh.lt.jst.dt.WordSimFromTermContext \
    --master=$master \
    --queue=$queue \
    --num-executors $num_executors \
    --driver-memory ${spark_gb}g \
    --executor-memory ${spark_gb}g \
    $bin_spark \
    $input \
    $wordsim \
    $WordsPerFeature \
    $FeaturesPerWord \
    $MinWordFreq \
    $MinFeatureFreq \
    $MinWordFeatureFreq \
    $MinFeatureSignif \
    $Significance \
    $NearestNeighboursNum
else
    echo "Unsupported format: $format"; exit 1
fi
