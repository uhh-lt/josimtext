hadoop=hadoop 
hadoop_xmx_mb=8192
hadoop_mb=8000
#spark=spark-submit
spark=/home/panchenko/bin/spark-2.0.2-bin-hadoop2.7/bin/spark-submit 
spark_gb=8
hadoop_conf_dir=/etc/hadoop/conf/
yarn_conf_dir=/etc/hadoop/conf.cloudera.yarn/
mwe_dict_path="voc/voc-mwe6446031-dbpedia-babelnet-wordnet-dela.csv" #"voc/voc-mwe-dela-wiki-druid-wordnet-babelnet-8m.csv" 
queue=default
master=local[*]

bin_spark=`ls ../bin/spark/jo*.jar`
bin_hadoop="../bin/hadoop/"
