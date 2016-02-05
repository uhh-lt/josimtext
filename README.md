JoBimSense
========

A system for noun sense induction and disambiguation. The system consist of several modules:

1. noun-sense-induction
2. noun-sense-induction-scala
3. chineese-whispers
4. wiki-link-preprocessor
5. wiki-wsd-task

This repository contains the module **noun-sense-induction-scala** that performs:
- construction of a distributional thesaurus from word-feature frequencies
- context clue aggregation
- evaluation 


Requirements
------------

- Git
- Maven
- Java >= 1.7
- SBT (for Scala project)
- Spark (if you want to run spark code from command line)

Download Code & Prepare Data
----------------------------

- https://github.com/tudarmstadt-lt/noun-sense-induction
- https://github.com/tudarmstadt-lt/noun-sense-induction-scala
- https://github.com/tudarmstadt-lt/chinese-whispers
- https://github.com/johannessimon/wiki-wsd-task
- Place your raw text (one sentence per line) on HDFS. We will reference this file/folder in the following as TEXT_PATH. The HDFS output folder will be referenced as WSI_OUT

Compile
-------

```
cd /path/to/noun-sense-induction-scala
sbt package
```

To run spark code:
```
spark-submit --master yarn-cluster [spark-options] --class [class-name] target/scala-2.10/noun-sense-induction_2.10-0.0.1.jar [args]
```

To avoid Spark errors, optionally increase timeouts on cluster (additional argument in spark-options):
```
--driver-java-options 
"-Dspark.core.connection.auth.wait.timeout=3600
-Dspark.core.connection.ack.wait.timeout=3600
-Dspark.akka.timeout=3600
-Dspark.storage.blockManagerSlaveTimeoutMs=360000
-Dspark.worker.timeout=360000
-Dspark.akka.retry.wait=360000
-Dspark.task.maxFailures=1"
```

Extract word/feature counts (noun-sense-induction)
------------------------

This extracts and counts words and word features (co-occurrences and dependencies) on a per-sentence basis
these frequencies are used both for DT computation, as well as for contextualization of sense clusters later on

Run the class "JoBimExtractAndCount" using Hadoop:

```
mvn-hadoop de.tudarmstadt.lt.wsi.JoBimExtractAndCount
-Dmapreduce.map.memory.mb=4096 
-Dmapreduce.task.io.sort.mb=1028
-Dmapreduce.local.map.tasks.maximum=4
-Dholing.dependencies=true
-Dholing.coocs=true
-Dmapred.max.split.size=1000000 
TEXT_PATH WSI_OUT/sentences-deps-coocs
```


- mapreduce.local.map.tasks.maximum --> maximum number of parallely executed local mappers
- holing.dependencies --> whether to write out dependency features (counts)
- holing.coocs --> whether to write out coocs (counts)
- mapred.max.split.size=1000000 --> one split is 1MB

Alternatively use the script: https://github.com/tudarmstadt-lt/joint/blob/master/run-nsi-hadoop.sh

Compute DT (noun-sense-induction-scala)
-----------

```
spark-submit 
--num-executors 260
--master yarn-cluster
--queue shortrunning
--driver-memory 7g
--executor-memory 1g
--class WordSimFromCounts
WSI_OUT/sentences-deps-coocs/DepWF-*
WSI_OUT/sentences-deps-coocs/W-*
WSI_OUT/sentences-deps-coocs/DepF-*
WSI_OUT/wordsim
100 0.0 2 10 2 LMI 3 100 100
```

- 100 --> compute 100 similar words per word
- 0.0 --> minimum feature significance is 0.0
- 2 --> use only features that were seen at least two times with every word
- 10 --> take only words seen at least 10 times
- 2 --> take only features seen at least 2 times (with any word)
- LMI --> use lexicographer's mutual information as significance score
- 3 --> round all similarities to 3 decimal places
- 100 --> use only 100 most significant features per word
- 100 --> compute only 100 most similar words per word

Alternatively use: https://github.com/tudarmstadt-lt/joint/blob/master/run-nsi-spark.sh

Sense Clustering (chinese-whispers)
-------

- Choose a local output folder to store data to (DATA_DIR)
- Save DT from HDFS to DATA_DIR/dt and run sense clustering:
```
hadoop fs -text WSI_OUT/wordsim/SimPruned/part* > DATA_DIR/dt
```

```
mvn-run de.tudarmstadt.lt.wsi.WSI
-clustering mcl
-in DATA_DIR/dt
-out DATA_DIR/dt_Clusters__e0__N010__n010-mcl
-N 10
-n 10
-e 0
```

- VM options (already set in mvn-run): -Xms8G -Xmx8G

Context Clue Aggregation (noun-sense-induction-scala)
----------

```
hadoop fs -put DATA_DIR/dt_Clusters__e0__N010__n010-mcl WSI_OUT/dt_Clusters__e0__N010__n010-mcl
```

**For co-occurrence clues:**
```
nohup spark-submit
--num-executors 200
--queue shortrunning
--master yarn-cluster
--class ClusterContextClueAggregator
--driver-memory 7g
--executor-memory 4g
WSI_OUT/dt_Clusters__e0__N010__n010-mcl
WSI_OUT/sentences-deps-coocs/W-*
WSI_OUT/sentences-deps-coocs/CoocF-*
WSI_OUT/sentences-deps-coocs/CoocWF-*
WithCoocs 10 2
```


**For dependency clues:**
```
nohup spark-submit
--num-executors 200
--queue shortrunning 
--master yarn-cluster
--class ClusterContextClueAggregator
--driver-memory 7g 
--executor-memory 4g
WSI_OUT/dt_Clusters__e0__N010__n010-mcl 
WSI_OUT/sentences-deps-coocs/W-*
WSI_OUT/sentences-deps-coocs/DepF-*
WSI_OUT/sentences-deps-coocs/DepWF-*
WithDeps 10 2```
```


Word Sense Disambiguation
----------------

```
spark-submit

--num-executors 20 --queue shortrunning --master yarn-cluster --class WSD --driver-memory 7g --executor-memory 7g --driver-java-options "-Dspark.storage.memoryFraction=0.1 -Dspark.shuffle.memoryFraction=0.1 -Dspark.core.connection.auth.wait.timeout=3600 -Dspark.core.connection.ack.wait.timeout=3600 -Dspark.akka.timeout=3600 -Dspark.storage.blockManagerSlaveTimeoutMs=360000 -Dspark.worker.timeout=360000 -Dspark.akka.retry.wait=360000 -Dspark.task.maxFailures=1 -Dspark.serializer=org.apache.spark.serializer.KryoSerializer"

target/scala-2.10/noun-sense-induction_2.10-0.0.1.jar
<SCORED-COOC-CLUES>
<SCORED-DEPENDENCY-CLUES>
<INSTANCES>
<OUTPUT>
0.00001 Product y
```

where <SCORED-COOC-CLUES> is a path on HDFS to the first file (...WithCoocs__twf2) and <SCORED-DEPENDENCY-CLUES> a path to the second file (...WithDeps__twf2). <OUTPUT> is the output path to write the result to (also on HDFS). "0.00001" is the smoothing, "Product" indicates that scores must be multiplicated, and the "y" for yes tells the classifier to take the "prior" score into account, i.e. the average cluster word frequency.

<INSTANCES> is the path of a file on HDFS containing the instances (to be sense-tagged) in the following format:

```
word <TAB> instance-id <TAB> coocs <TAB> deps
```

where instance-id is simply a unique ID for every instance, coocs is the sentence/context as a lemmatized set of words and deps is the comma-separated list of dependency features of the head word (e.g. "amod(@@,wild)").

