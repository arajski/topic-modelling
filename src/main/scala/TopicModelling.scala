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

    def runLDA(df: DataFrame, sparkSession: SparkSession, file: String) = {
        import sparkSession.implicits._

        val tweets = df.select("text")

        //tokenizing documents
        val tokens = NLPHelper.processDocuments(tweets,"text")
        val nouns = NLPHelper.selectNouns(tokens)
        //removing stopwords
        val filtered = nouns.map(t => StopWordsHelper.removeStopWords(t))
        
        //counting tokens
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
                            .setMaxIter(300)
                            .setCheckpointInterval(100)
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

    def main(args: Array[String]) {
        // initialise spark context
        val conf = new SparkConf().setAppName("TopicModelling")
        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder.getOrCreate()
        import sparkSession.implicits._
        val url = "hdfs://localhost:9000/hdfs/data"
        val hdfsFiles = FileSystem.get(new URI(url), sc.hadoopConfiguration).listStatus(new Path(url))

        for (file <- hdfsFiles) {
            val filePath = file.getPath().toString()
            val resultName = filePath.slice(filePath.indexOf("/data")+6,filePath.length-4)

            val documents = sparkSession.read.json(filePath)
            runLDA(documents,sparkSession,"results/"+resultName+"_results.txt")
        }

        sc.stop()

    }
}

