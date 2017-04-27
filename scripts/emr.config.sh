
# The binaries should be stored in the JoSimText/bin/{spark,hadoop}
# The run is expected to be from JoSimText directory, e.g. ./scripts/conll.sh

hadoop=hadoop
hadoop_xmx_mb=8192
hadoop_mb=8000
mwe_dict_path=s3://jobimtext/voc/voc-mwe6446031-dbpedia-babelnet-wordnet-dela.csv  
spark=spark-submit 
spark_gb=64 #8
export HADOOP_CONF_DIR=/etc/hadoop/conf/
export YARN_CONF_DIR=/etc/hadoop/conf/
queue=default

bin_spark=`ls ../bin/spark/jo*.jar`
bin_hadoop="../bin/hadoop/"
