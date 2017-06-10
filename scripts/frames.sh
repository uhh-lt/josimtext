#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "parameters: <conll-dir> <output-dir> <config.sh>"
    exit
fi

input_dir=$1
output_dir=$2
config=$3

source $config

spark-submit \
    --class Conll2Features \
    --master=$master \
    --queue=$queue \
    --driver-memory 24g \
    --executor-memory 32g \
    $bin_spark \
    $input_dir \
    $output_dir \
    true
