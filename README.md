JoSimText
========

This system performs [word sense induction](https://en.wikipedia.org/wiki/Word-sense_induction) form text. This is an implementation of the [JoBimText](http://www.jobimtext.org) approach 
 in Scala, Spark, tuned for induction of word Senses (hence the "S" instead of "B" in the name, but also because of the
 name of the initial developer of the project Johannes Simon). The [original JoBimText implementation](http://maggie.lt.informatik.tu-darmstadt.de/jobimtext/downloads/) is written 
 in Java/Pig and is more generic as it supposes that "Jo"s (i.e. objects) and "Bims (i.e. features) can be any linguistic objects. This particular implementation is designed for modeling of 
  words and multiword expressions. 


The system consist of several modules:

1. [Term feature extraction](https://github.com/uhh-lt/lefex)
2. Term similarity (this reposiroty). This repository performs construction of a distributional thesaurus from word-feature frequencies.
3. [Word sense induction](https://github.com/uhh-lt/chinese-whispers)


System requirements:
------------

1. git
2. Java 1.8+
3. Apache Spark 2.2+


Installation of the tool:
-----------

1. Get the source code:

```shell
git clone https://github.com/uhh-lt/josimtext.git
cd josimtext
```

2. Build the tool:

```shell
make
```

3. Set the environment variable ```SPARK_HOME``` to the directory with Spark installation.

Run a command:
------------

1. To see the list of available commands:
```shell
./run
```

2. To see arguments of a particular command, e.g. :

```shell
./run WordSimFromTermContext --help
```

3. By default, the tool is running locally. To change Spark and Hadoop parameters of the job (queue, number of executors, memory per job, and so on) you need to modify the ```conf/env.sh``` file. A sample file for running the jobs using the CDH YARN cluster are provided in ```conf/cdh.sh```. 
