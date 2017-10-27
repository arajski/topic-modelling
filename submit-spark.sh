#!/bin/bash
mkdir -p lib
if [ ! -f lib/stanford-corenlp-3.6.0.jar ]; then
    echo "Downloading stanford-corenlp-3.6.0"
    wget --progress=bar http://nlp.stanford.edu/software/stanford-corenlp-full-2015-12-09.zip
    unzip stanford-corenlp-full-2015-12-09.zip
    cd stanford-corenlp-full-2015-12-09
    mv ejml-0.23.jar ../lib
    mv joda-time.jar ../lib
    mv jollyday.jar ../lib
    mv protobuf.jar ../lib
    mv slf4j-simple.jar ../lib
    mv stanford-corenlp-3.6.0-models.jar ../lib
    mv stanford-corenlp-3.6.0.jar ../lib
    mv xom.jar ../lib
    cd ..
    rm -rf stanford-corenlp-full-2015-12-09
    rm stanford-corenlp-full-2015-12-09.zip
    echo "Downloading spark-corenlp-0.2.0-s_2.11"
    wget --progress=bar https://dl.bintray.com/spark-packages/maven/databricks/spark-corenlp/0.2.0-s_2.11/spark-corenlp-0.2.0-s_2.11.jar -P lib
fi
JARFILE=`pwd`/target/scala-2.11/TopicModelling-assembly-1.0.jar
if [ ! -f "$JARFILE" ]; then
    echo "Compiling application..."
    sbt -J-Xms2048m -J-Xmx2048m assembly
fi
# Directory where spark-submit is defined
# Install spark from https://spark.apache.org/downloads.html
export HADOOP_CONF_DIR=/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop
SPARK_HOME=`pwd`/../spark-2.1.0-bin-hadoop2.6
JARS=$(echo "lib"/*.jar | tr ' ' ',')
# JAR containing class
echo "Submitting application to Spark..."
${SPARK_HOME}/bin/spark-submit --class TopicModelling --master local[*] $JARFILE $1 #--master local[*] $JARFILE
