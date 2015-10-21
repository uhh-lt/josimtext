#!/bin/bash

corpus=$1
output=$2

features="$output-features" 
wordsim="$output-wordsim"

time ./run-nsi-hadoop.sh $corpus $features 
time ./run-nsi-spark.sh $features $wordsim
