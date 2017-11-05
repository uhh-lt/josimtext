#!/usr/bin/env bash

set -o nounset # Error on referencing undefined variables, shorthand: set -n
set -o errexit # Abort on error, shorthand: set -e

if [ $# -lt 3 ]; then
    echo "Compute a similarity score between two DTs."
    echo "parameters: <first-dt-path> <first-dt-path> <config.sh>"
    exit
fi

# System parameters
source $3

first_path=${1}
second_path=${2}

echo "First DT path: $first_path"
echo "Second DT path: $first_path"
echo "To start press any key, to stop press Ctrl+C"
read -n 2

$spark \
--class=de.uhh.lt.jst.dt.benchmarks.MeasureDTSim \
--master=$master \
--queue=$queue \
--num-executors $num_executors \
--driver-memory ${spark_gb}g \
--executor-memory ${spark_gb}g \
$bin_spark \
$first_path \
$second_path