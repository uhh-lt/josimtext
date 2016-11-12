if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Split too long lines in the input corpus"
    echo "parameters: <corpus-directory> <output-directory> <queue>"
    echo "<queue>  shortrunning, longrunning"
    exit
fi


# Process input params
corpus=$1
output=$2
queue=$3
make_uniq=true
compress_output=false

bin=/home/panchenko/JoSimText/bin/hadoop

jars=`echo $bin/*.jar | tr " " ","`
path=`echo $bin/*.jar | tr " " ":"`

HADOOP_CLASSPATH=$path hadoop \
    de.tudarmstadt.lt.jst.SentenceSplitter.HadoopMain \
    -libjars $jars \
    -Dmapreduce.reduce.failures.maxpercent=10 \
    -Dmapreduce.map.failures.maxpercent=10 \
    -Dmapreduce.job.queuename=$queue\
    -Dmapreduce.map.java.opts=-Xmx4g \
    -Dmapreduce.map.memory.mp=4096 \
    -Dmapreduce.task.io.sort.mb=1028 \
    -Dmapred.max.split.size=1000000 \
    -Dtokenize=true \
    -Dmax_sentences_size=150 \
    $corpus \
    $output \
    $make_uniq \
    $compress_output
