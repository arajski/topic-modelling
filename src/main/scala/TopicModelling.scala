import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.parsing.json._

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.functions._


object TopicModelling {
    
    def formatTweet(tweet: Any) = {
        var formattedTweet = tweet.asInstanceOf[String]
            .replaceAll("http[^\\s]+", " ");
        formattedTweet
    }
    def main(args: Array[String]) {

        // initialise spark context
        val conf = new SparkConf().setAppName("TopicModelling")
        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder.getOrCreate()
        import sparkSession.implicits._
        val df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/*")
        
        //selecting only text element of tweets
        val tweets = df.select("text").map(tweet => formatTweet(tweet(0)))

        //tokenizing tweet
        val tokenizer = new Tokenizer().setInputCol("value").setOutputCol("words")
        val tokenized = tokenizer.transform(tweets)

        //removing stopwords
        val remover = new StopWordsRemover()
            .setInputCol("words")
            .setOutputCol("filtered")

        val filtered = remover.transform(tokenized)

        //counting tokens
        val cvModel: CountVectorizerModel = new CountVectorizer()
          .setInputCol("filtered")
          .setOutputCol("features")
          .fit(filtered)

        val vectorized = cvModel.transform(filtered)
        val parsedData = vectorized.select("features")

        //obtaining topics
        val lda = new LDA().setK(10).setMaxIter(10)
        val model = lda.fit(parsedData)

        // Describe topics.
        val topics = model.describeTopics(3)
        println("The topics described by their top-weighted terms:")
        topics.show(false)

        val transformed = model.transform(parsedData)
        transformed.show()

        sc.stop()
    
    }
}

