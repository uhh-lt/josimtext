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
    --class FilterSentences \
    --master=$master \
    --queue=$queue \
    --driver-memory 8g \
    --executor-memory 4g \
    $bin_spark \
    $input_dir \
    $output_dir

   # --conf spark.yarn.max.executor.failures    
   # --num-executors 100 \
