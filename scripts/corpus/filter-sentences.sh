#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Filter noisy sentences."
    echo "parameters: <input-directory> <output-directory> <config.sh>"
    exit
fi

input_dir=$1
output_dir=$2
config=$3

source $config

$spark \
    --class de.uhh.lt.jst.corpus.FilterSentences \
    --master=$master \
    --queue=$queue \
    --driver-memory 8g \
    --executor-memory 4g \
    --num-executors 100 \
    $bin_spark \
    $input_dir \
    $output_dir

