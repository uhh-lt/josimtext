if [ -z "$1" ] || [ -z "$4" ] ; then
    echo "Run word sense induction"
    echo "parameters: <corpus-directory> <output-directory> <do-calc-features> <do-calc-sims> <queue>"
    echo "<queue>  shortrunning, longrunning"
    exit
fi

source features-n-sims.config.sh
bin_spark=`ls ../bin/spark/jo*.jar`
bin_hadoop="../bin/hadoop/"


# Feature extraction parameters
holing_type="dependency" # "trigram" 
lemmatize=true # false
coocs=false # false # true
maxlen=110
noun_noun_only=false # true
semantify=true
mwe_via_ner=true # false
mwe_self_features=false
parser="malt" # "stanford" "malt", "mate"

# Term similarity
WordsPerFeature=1000 # 100 1000 10000
FeaturesPerWord=1000 # 100 1000 10000
MinWordFreq=5
MinFeatureFreq=5
MinWordFeatureFreq=2
Significance=LMI
MinFeatureSignif=0.0
NearestNeighboursNum=200

# Process input params
corpus=$1
output=$2
calc_features=$3
calc_sims=$4
queue=$5
features="$output/Holing-${holing_type}_Lemmatize-${lemmatize}_Coocs-${coocs}_MaxLen-${maxlen}_NounNounOnly-${noun_noun_only}_Semantify-${semantify}" 
wordFeatureCountsFile=$features/WF-* 
wordCountsFile=$features/W-* 
featureCountsFile=$features/F-* 
wordsim="${features}__Significance-${Significance}_WordsPerFeature-${WordsPerFeature}_FeaturesPerWord-${FeaturesPerWord}_MinWordFreq-${MinWordFreq}_MinFeatureFreq-${MinFeatureFreq}_MinWordFeatureFreq-${MinWordFeatureFreq}_MinFeatureSignif-${MinFeatureSignif}_SimPrecision-${SimPrecision}_NearestNeighboursNum-${NearestNeighboursNum}"

# Display job parameters to the user
echo "Corpus: $corpus"
if  $hadoop fs -test -e $corpus  ; then
    echo "Corpus exists: true"
else
    echo "Corpus exists: false"
fi
echo ""

echo "Output: $output"
echo ""

echo "Features: $features"
echo "Calculate features: $calc_features"
if  $hadoop fs -test -e $features ; then
    echo "Features exist: true"
else
    echo "Features exist: false"
fi
echo ""

echo "Similarities: $wordsim"
echo "Calculate similarities: $calc_sims"
wordsim=${wordsim:0:254} # to meet 255 limit of HDFS filename
if  $hadoop fs -test -e $wordsim  ; then
    echo "Similarity exist: true"
else
    echo "Similarity exist: false"
fi
echo ""

echo "To start press any key, to stop press Ctrl+C"
read -n 2

# Create output directory
if ! $hadoop fs -test -e $output  ; then
    $hadoop fs -mkdir $output
    echo "Created output: '$output'"
fi


# Generate word-feature matrix
if $calc_features; then  
    if $hadoop fs -test -e $features ; then
        if [ ${#features} > 40 ] ; then
            echo "Deleting: $features"
            $hadoop fs -rm -r $features
        fi
    fi
    echo "Calculating features ..."

    jars=`echo $bin_hadoop/*.jar | tr " " ","`
    path=`echo $bin_hadoop/*.jar | tr " " ":"`

    HADOOP_CLASSPATH=$path $hadoop \
        de.tudarmstadt.lt.jst.ExtractTermFeatureScores.HadoopMain \
        -libjars $jars \
        -Dmapreduce.reduce.failures.maxpercent=10 \
        -Dmapreduce.map.failures.maxpercent=10 \
        -Dmapreduce.job.queuename=$queue\
        -Dmapreduce.map.java.opts=-Xmx${hadoop_xmx_mb}m \
        -Dmapreduce.map.memory.mb=$hadoop_mb \
        -Dmapreduce.reduce.java.opts=-Xmx${hadoop_xmx_mb}m \
        -Dmapreduce.reduce.memory.mb=$hadoop_mb \
        -Dmapred.max.split.size=1000000 \
        -Dholing.type=$holing_type \
        -Dholing.coocs=$coocs \
        -Dholing.dependencies.semantify=$semantify \
        -Dholing.sentences.maxlength=$maxlen \
        -Dholing.lemmatize=$lemmatize \
        -Dholing.dependencies.noun_noun_dependencies_only=$noun_noun_only \
        -Dholing.mwe.vocabulary=$mwe_dict_path \
        -Dholing.mwe.self_features=$mwe_self_features \
        -Dholing.mwe.ner=$mwe_via_ner \
        -Dholing.dependencies.parser=$parser \
        $corpus \
        $features 
fi 

# Calculate word similarities
if $calc_sims; then  
    if $hadoop fs -test -e $wordsim ; then
        if [[ ${#wordsim} > 50 ]] ; then
            echo "Deleting: $wordsim"
            $hadoop fs -rmr $wordsim
        fi
    fi
    echo "Calculating similarities..." 

    export HADOOP_CONF_DIR=$hadoop_conf_dir
    export YARN_CONF_DIR=$yarn_conf_dir

    $spark \
        --class=WordSimFromCounts \
        --master=yarn \
        --queue=$queue \
        --num-executors 50 \
        --driver-memory ${spark_gb}g \
        --executor-memory ${spark_gb}g \
        $bin_spark \

        $wordCountsFile \
        $wordFeatureCountsFile \
        $featureCountsFile \
        $wordsim \
        $WordsPerFeature \
        $FeaturesPerWord \
        $MinWordFreq \
        $MinFeatureFreq \
        $MinWordFeatureFreq \
        $MinFeatureSignif \
        $Significance \
        $NearestNeighboursNum
fi

val significanceMin = if (args.size > 9) args(9).toDouble else significanceMinDefault
val significanceType = if (args.size > 10) args(10) else significanceTypeDefault
val similarWordsMaxNum = if (args.size > 11) args(11).toInt else similarWordsMaxNumDefault