# requires git, mvn, sbt, scala 

mkdir JoSimText
cd JoSimText

# download hadoop and spark for local tests
wget http://apache.lauf-forum.at/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar xzf hadoop-2.7.3.tar.gz 
rm hadoop-2.7.3.tar.gz
mv hadoop-2.7.3 hadoop

wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.1-bin-hadoop2.7.tgz
tar xzf spark-2.0.1-bin-hadoop2.7.tgz
rm spark-2.0.1-bin-hadoop2.7.tgz
mv spark-2.0.1-bin-hadoop2.7 spark

# build feature extraction
git clone https://github.com/tudarmstadt-lt/lefex.git
cd lefex
mvn package
mvn dependency:go-offline

# build similarity 
cd ..
git clone https://github.com/tudarmstadt-lt/JoSimText.git
cd JoSimText
sbt package

# copy binaries
mkdir bin
mkdir bin/hadoop
mkdir bin/spark
cp target/scala-*/*jar bin/spark 
cp ../lefex/target/*jar bin/hadoop
cp ../lefex/target/lib/*jar bin/hadoop
