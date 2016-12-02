#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Coarsify POS tags."
    echo "parameters: <word-feature-counts-directory> <output-directory> <queue>"
    exit
fi

input_dir=$1
output_dir=$2
queue=$3
bin_spark=`ls ../bin/spark/jo*.jar`

spark-submit \
    --class CoarsifyPosTags \
    --master=yarn \
    --queue=$queue \
    --num-executors 100 \
    --driver-memory 8g \
    --executor-memory 4g \
    $bin_spark \
    $input_dir \
    $output_dir
