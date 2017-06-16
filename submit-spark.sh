#!/bin/bash
echo "Compiling application..."
sbt -J-Xms2048m -J-Xmx2048m assembly
#sbt test
# Directory where spark-submit is defined
# Install spark from https://spark.apache.org/downloads.html
export HADOOP_CONF_DIR=/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop
SPARK_HOME=`pwd`/../spark-2.1.0-bin-hadoop2.6
JARS=$(echo "lib"/*.jar | tr ' ' ',')
# JAR containing class
JARFILE=`pwd`/target/scala-2.11/TopicModelling-assembly-1.0.jar

#rm $JARFILE
# Run it locally

${SPARK_HOME}/bin/spark-submit --class TopicModelling --deploy-mode cluster --master yarn $JARFILE hdfs://localhost:9000/hdfs/data #--master local[*] $JARFILE
#${SPARK_HOME}/bin/spark-submit --class TopicModelling s3://aws-logs-503617471175-us-west-2/LDA/TopicModelling-assembly-1.0.jar
