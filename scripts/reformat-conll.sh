#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "parameters: <conll-old> <conll-new> <config.sh>"
    exit
fi

input_dir=$1
output_dir=$2
config=$3

source $config

spark-submit \
    --class de.uhh.lt.jst.corpus.ReformatConll \
    --master=$master \
    --queue=$queue \
    --driver-memory 8g \
    --executor-memory 4g \
    $bin_spark \
    $input_dir \
    $output_dir \
    true
