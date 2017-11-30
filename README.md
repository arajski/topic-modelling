# Topic Modelling

The main focus of this project is the application of Topic Modeling on short documents, collected from social media platform - Twitter.
The algorithm used for this purpose is Latent Dirichlet Allocation, which is one of the simplest topic models. 
Apache Spark engine together with its underlying Hadoop File System have been used to distribute work across all nodes/machines.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites

- [x] SBT 0.13.12
- [x] Apache Spark 2.1.0
- [x] Scala 2.11.0
- [x] Hadoop 2.7.3

Dataset has been collected from Twitter platform using [TwitterCollector](https://github.com/arajski/various-scripts/tree/master/twitterCollector) script.

### Installing

1. Run following commands for initial project setup:

```
git clone http://github.com/arajski/topic-modelling
cd topic-modelling
```
2. Edit `submit-spark.sh` file to make sure it contains correct paths for Hadoop and Apache Spark (file contains sample configuration). 
3. To run the application and send it to Apache Spark cluster, execute the following script with HDFS url as a parameter. 
It should point to a directory with stored data files.
```
./submit-spark.sh hdfs_url
```
First run will download all dependencies, including [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/) library, 
compile the solution and run the test suites.
## Running the tests

To run the test suites, simply run `sbt test`.
Test cases are available in `src/test/scala` directory

## Built With

* [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/) - Library used for Natural Language Processing
* [Apache Spark](https://spark.apache.org/) - Data processing and task distribution engine


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

