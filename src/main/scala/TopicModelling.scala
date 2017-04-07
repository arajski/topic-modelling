import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.parsing.json._
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.functions._

import java.io._


object TopicModelling {

    def runLDA(df: DataFrame, sparkSession: SparkSession, file: String) = {
        import sparkSession.implicits._

        val tweets = df.select("text").map(tweet => formatTweet(tweet(0)))

        //tokenizing tweet
        val tokenizer = new Tokenizer().setInputCol("value").setOutputCol("words")
        val tokenized = tokenizer.transform(tweets).select("words")
            .map(w => w(0).asInstanceOf[Seq[String]].filter(_.trim.length > 2)).toDF("words")
        //removing stopwords
        //StopWordsHelper.removeStopWords()
        val remover = new StopWordsRemover()
            .setInputCol("words")
            .setOutputCol("filtered")
        val filtered = remover.transform(tokenized)
        //counting tokens
        val cvModel: CountVectorizerModel = new CountVectorizer()
            .setInputCol("filtered")
            .setOutputCol("features")
            .fit(filtered)
        val vocabularyArray = cvModel.vocabulary

        val vectorized = cvModel.transform(filtered)
        val parsedData = vectorized.select("features")

        //obtaining topics
        val lda = new LDA().setK(5)
        val model = lda.fit(parsedData)

        // Describe topics.
        val topics = model.describeTopics(10)
        //Save topics to file
        printTopics(topics,file,vocabularyArray)
        
    }
    def printTopics(topics: DataFrame, filename: String, vocabularyArray: Array[String]) = {

        val file = new File(filename)
        val fw = new BufferedWriter(new FileWriter(file,true))

        topics.collect().foreach { 
            r => {
                fw.write("===================\n")    
                fw.write("Topic:" + r(0) + "\n")
                fw.write("===================\n")  
                r(1).asInstanceOf[Seq[Int]].foreach {
                    t => { fw.write(vocabularyArray(t) + "\n") }
                }
            }
        }   

        fw.close()

    }
    def formatTweet(tweet: Any) = {
        var formattedTweet = tweet.asInstanceOf[String]
        .replaceAll("http[^\\s]+", " ")
        .replaceAll("[^\\p{L}\\p{Nd}]+", " ")
        .replaceAll("[^\u0000-\uFFFF]", " ")
        formattedTweet
    }

    def main(args: Array[String]) {

        // initialise spark context
        val conf = new SparkConf().setAppName("TopicModelling")
        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder.getOrCreate()
        import sparkSession.implicits._
        //var df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct21.txt")
        //runLDA(df,sparkSession,"oct21_results.txt")
        
        /*df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct22.txt")
        runLDA(df,sparkSession,"oct22_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct23.txt")
        runLDA(df,sparkSession,"oct23_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct24.txt")
        runLDA(df,sparkSession,"oct24_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct25.txt")
        runLDA(df,sparkSession,"oct25_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct26.txt")
        runLDA(df,sparkSession,"oct26_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct27.txt")
        runLDA(df,sparkSession,"oct27_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct28.txt")
        runLDA(df,sparkSession,"oct28_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov01.txt")
        runLDA(df,sparkSession,"Nov01_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov02.txt")
        runLDA(df,sparkSession,"Nov02_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov03.txt")
        runLDA(df,sparkSession,"Nov03_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov04.txt")
        runLDA(df,sparkSession,"Nov04_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov05.txt")
        runLDA(df,sparkSession,"Nov05_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov06.txt")
        runLDA(df,sparkSession,"Nov06_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov07.txt")
        runLDA(df,sparkSession,"Nov07_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov08.txt")
        runLDA(df,sparkSession,"Nov08_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov09.txt")
        runLDA(df,sparkSession,"Nov09_results.txt")

        df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Nov10.txt")
        runLDA(df,sparkSession,"Nov10_results.txt")*/
       // val oct25 = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/Oct25.txt")

        sc.stop()

    }
}

