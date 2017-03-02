#!/bin/bash

echo "Compiling application..."
sbt package

# Directory where spark-submit is defined
# Install spark from https://spark.apache.org/downloads.html
SPARK_HOME=`pwd`/../spark-2.1.0-bin-hadoop2.6

# JAR containing class
JARFILE=`pwd`/target/scala-2.11/topicmodelling_2.11-1.0.jar
#rm $JARFILE
# Run it locally
${SPARK_HOME}/bin/spark-submit --class TopicModelling --master local $JARFILE
