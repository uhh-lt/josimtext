hadoop=hadoop 
hadoop_xmx_mb=8192
hadoop_mb=8000
mwe_dict_path=s3://jobimtext/voc/voc.csv  
spark=spark-submit 
spark_gb=8
export HADOOP_CONF_DIR=/etc/hadoop/conf/
export YARN_CONF_DIR=/etc/hadoop/conf/
queue=default

bin_spark=`ls ../bin/spark/jo*.jar`
bin_hadoop="../bin/hadoop/"
