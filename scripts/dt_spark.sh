#!/usr/bin/env bash

set -o nounset # Error on referencing undefined variables, shorthand: set -n
set -o errexit # Abort on error, shorthand: set -e

if [ $# -lt 4 ]; then
    echo "Compute a DT from different input formats (conll, corpus, termcontext)"
    echo "parameters: <format> <input-directory> <output-directory> <config.sh>"
    exit
fi

format=${1}
input=${2}
output=${3}

# System parameters
source $4

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
    $output
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
    $output
else
    echo "Format $format, currently not implemented"
fi