spark-submit --class FreqFilter --master=yarn-cluster --queue=shortrunning --num-executors 10 --driver-memory 7g --executor-memory 7g ~/bin-spark/nsi_2.10-0.0.1.jar freq-wiki.csv voc-wordnet3.csv freq-wordnet-test false

spark-submit --class FreqFilter --master=yarn-cluster --queue=shortrunning --num-executors 200 --driver-memory 7g --executor-memory 7g ~/bin-spark/nsi_2.10-0.0.1.jar corpora/en/wikipedia_eugen_mwe_trigram__WordCount voc/voc-mwe-dela-wiki-druid-wordnet-63m.csv freq-wikipedia true

spark-submit --class FreqFilter --master=yarn-cluster --queue=longrunning --num-executors 200 --driver-memory 7g --executor-memory 7g ~/bin-spark/nsi_2.10-0.0.1.jar corpora/en/59g-spl_mwe_trigram__WordCount voc/voc-mwe-dela-wiki-druid-wordnet-63m.csv freq-59g true
