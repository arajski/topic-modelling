import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.parsing.json._

object TopicModelling {

    def main(args: Array[String]) {

        // initialise spark context
        val conf = new SparkConf().setAppName("TopicModelling")
        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder.getOrCreate()
        val df = sparkSession.read.json("hdfs://localhost:9000/hdfs/data/*")
        
        //selecting only text element of tweets
        val tweetTexts = df.select("text")
        
        // terminate spark context
        sc.stop()
    
    }
}

