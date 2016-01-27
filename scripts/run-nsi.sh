if [ -z "$1" ] || [ -z "$4" ] ; then
    echo "Run noun sense induction"
    echo "parameters: <corpus-directory> <output-directory> <do-calc-features> <do-calc-sims> <queue>"
    echo "<queue>  shortrunning, longrunning"
    exit
fi

# Meta-paramters of feature extraction
holing_type="dependency"
lemmatize=true
coocs=false
maxlen=110
nouns_only=false
noun_noun_only=false
semantify=true

# Meta-parameters of similarity
Significance=LMI
WordsPerFeature=1000 
MinFeatureSignif=0.0
MinWordFeatureFreq=2
MinWordFreq=5
MinFeatureFreq=5
SimPrecision=5
FeaturesPerWord=1000 
NearestNeighboursNum=200 

# Process input params
corpus=$1
output=$2
calc_features=$3
calc_sims=$4
queue=$5
features="$output/Holing-${holing_type}_Lemmatize-${lemmatize}_Coocs-${coocs}_MaxLen-${maxlen}_NounsOnly-${nouns_only}_NounNounOnly-${noun_noun_only}_Semantify-${semantify}" 
wordFeatureCountsFile=$features/WF-* 
wordCountsFile=$features/W-* 
featureCountsFile=$features/F-* 
wordsim="${features}__Significance-${Significance}_WordsPerFeature-${WordsPerFeature}_FeaturesPerWord-${FeaturesPerWord}_MinWordFreq-${MinWordFreq}_MinFeatureFreq-${MinFeatureFreq}_MinWordFeatureFreq-${MinWordFeatureFreq}_MinFeatureSignif-${MinFeatureSignif}_SimPrecision-${SimPrecision}_NearestNeighboursNum-${NearestNeighboursNum}"


# Display job parameters to the user
echo "Corpus: $corpus"
if  hadoop fs -test -e $corpus  ; then
    echo "Corpus exists: true"
else
    echo "Corpus exists: false"
fi
echo ""

echo "Output: $output"
echo ""

echo "Features: $features"
echo "Calculate features: $calc_features"
if  hadoop fs -test -e $features ; then
    echo "Features exist: true"
else
    echo "Features exist: false"
fi
echo ""

echo "Similarities: $wordsim"
echo "Calculate similarities: $calc_sims"
wordsim=${wordsim:0:254} # to meet 255 limit of HDFS filename
if  hadoop fs -test -e $wordsim  ; then
    echo "Similarity exist: true"
else
    echo "Similarity exist: false"
fi
echo ""

echo "To start press any key, to stop press Ctrl+C"
read -n 2

# Create output directory
if ! hadoop fs -test -e $output  ; then
    hadoop fs -mkdir $output
    echo "Created output: '$output'"
fi


# Generate word-feature matrix
if $calc_features; then  
    if hadoop fs -test -e $features ; then
        if [ ${#features} > 40 ] ; then
            echo "Deleting: $features"
            hadoop fs -rm -r $features
        fi
    fi
    echo "Calculating features ..."
    bin=/home/panchenko/bin-hadoop

    jars=`echo $bin/*.jar | tr " " ","`
    path=`echo $bin/*.jar | tr " " ":"`

    HADOOP_CLASSPATH=$path hadoop \
        de.tudarmstadt.lt.wsi.JoBimExtractAndCount \
        -libjars $jars \
        -Dmapreduce.reduce.failures.maxpercent=10 \
        -Dmapreduce.map.failures.maxpercent=10 \
        -Dmapreduce.job.queuename=$queue\
        -Dmapreduce.map.java.opts=-Xmx8192m \
        -Dmapreduce.reduce.java.opts=-Xmx8192m \
        -Dmapreduce.map.memory.mp=9000 \
        -Dmapreduce.task.io.sort.mb=9000 \
        -Dmapreduce.reduce.memory.mb=9000 \
        -Dmapred.max.split.size=1000000 \
        -Dholing.type=$holing_type \
        -Dholing.coocs=$coocs \
        -Dholing.nouns_only=$nouns_only \
        -Dholing.dependencies.semantify=$semantify \
        -Dholing.sentences.maxlength=$maxlen \
        -Dholing.lemmatize=$lemmatize \
        -Dholing.dependencies.noun_noun_dependencies_only=$noun_noun_only \
        $corpus \
        $features 
fi 

# Calculate word similarities
if $calc_sims; then  
    if hadoop fs -test -e $wordsim ; then
        if [[ ${#wordsim} > 50 ]] ; then
            echo "Deleting: $wordsim"
            hadoop fs -rmr $wordsim
        fi
    fi
    echo "Calculating similarities..." 
    bin=/home/panchenko/bin-spark

    export HADOOP_CONF_DIR=/etc/hadoop/conf/
    export YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn/

    /usr/bin/spark-submit \
        --class=WordSimFromCounts \
        --master=yarn-cluster \
        --queue=$queue \
        --num-executors 50 \
        --driver-memory 8g \
        --executor-memory 8g \
        $bin/noun-sense-induction_2.10-0.0.1.jar \
        $wordFeatureCountsFile \
        $wordCountsFile \
        $featureCountsFile \
        $wordsim \
        $WordsPerFeature \
        $MinFeatureSignif \
        $MinWordFeatureFreq \
        $MinWordFreq \
        $MinFeatureFreq \
        $Significance \
        $SimPrecision \
        $FeaturesPerWord \
        $NearestNeighboursNum
fi
