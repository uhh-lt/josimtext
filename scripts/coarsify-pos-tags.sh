#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Coarsify POS tags."
    echo "parameters: <word-feature-counts-directory> <output-directory> <config.sh>"
    exit
fi

input_dir=$1
output_dir=$2
config=$3

source $config 

spark-submit \
    --class de.uhh.lt.jst.corpus.CoarsifyPosTags \
    --master=yarn \
    --queue=$queue \
    --num-executors 100 \
    --driver-memory 8g \
    --executor-memory 4g \
    $bin_spark \
    $input_dir \
    $output_dir
