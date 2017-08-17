#!/usr/bin/env bash

set -o nounset # Error on referencing undefined variables, shorthand: set -n
set -o errexit # Abort on error, shorthand: set -e

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Compute a term context pairs from CoNLL"
    echo "parameters: <input-directory> <output-directory> <config.sh>"
    exit
fi

input=${1}
output=${2}

# System parameters
source $3

echo "Input: $input"
echo "Output: $output"
echo "To start press any key, to stop press Ctrl+C"
read -n 2

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