import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.parsing.json._
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem,FileStatus,Path}
import java.net.URI;
import java.io._


object TopicModelling {

    def runLDA(df: DataFrame, sparkSession: SparkSession, file: String, url: String) = {
        import sparkSession.implicits._

        val tweets = df.select("text")

        val tokens = NLPHelper.processDocuments(tweets,"text",sparkSession)
        val nouns = NLPHelper.selectNouns(tokens,sparkSession)

        val filtered = nouns.map(t => StopWordsHelper.removeStopWords(t))
        
        val cvModel: CountVectorizerModel = new CountVectorizer()
            .setInputCol("value")
            .setOutputCol("features")
            .fit(nouns)
        val vocabularyArray = cvModel.vocabulary

        val vectorized = cvModel.transform(filtered)
        val parsedData = vectorized.select("features")

        //running LDA
        val lda = new LDA().setSeed(80)
                            .setK(6)
        val model = lda.fit(parsedData)

        // Describe topics.
        val topics = model.describeTopics(10)
        //Save topics to file
        printTopics(topics,file,vocabularyArray,sparkSession,url)
        
    }
    def printTopics(topics: DataFrame, filename: String, vocabularyArray: Array[String],sparkSession: SparkSession, url:String) = {
        import sparkSession.implicits._
        val file = new File(filename)
        val fw = new BufferedWriter(new FileWriter(file,true))
        var topicDF = Seq[String]()
        topics.collect().foreach { 
            r => {
                topicDF = topicDF :+ "==============="
                r(1).asInstanceOf[Seq[Int]].foreach {
                    t => {topicDF = topicDF :+ vocabularyArray(t)}
                }
            }
        }   
        topicDF.toDF().coalesce(1).rdd.saveAsTextFile(url + "/" + filename)
        fw.close()

    }

    def main(args: Array[String]) {
        // initialise spark context
        val conf = new SparkConf().setAppName("TopicModelling")
        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder.getOrCreate()
        import sparkSession.implicits._

        val url = args(0)
        val hdfsFiles = FileSystem.get(new URI(url), sc.hadoopConfiguration).listStatus(new Path(url))

        for (file <- hdfsFiles) {
            val filePath = file.getPath().toString()
            val resultName = filePath.slice(filePath.indexOf("/data")+6,filePath.length-4)

            val documents = sparkSession.read.json(filePath)

            runLDA(documents,sparkSession,resultName+"_results.txt",url)
        }

        sc.stop()

    }
}

