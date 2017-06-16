import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}

object NLPHelper {

	def processDocuments(documents: DataFrame, column: String, sparkSession: SparkSession) : Dataset[(Seq[String], Seq[String])] = {
   		import sparkSession.implicits._

		val validDocuments = removeInvalidCharacters(documents, column,sparkSession)
		val output = validDocuments.select(lemma(lower('value)).as('words),
											pos(lower('value)).as('pos))

		output.toDF("tokens","pos").as[(Seq[String],Seq[String])]
	}

	def removeInvalidCharacters(df: DataFrame, column: String, sparkSession: SparkSession) : Dataset[String] = {
   		import sparkSession.implicits._

		df.select(col(column)).map(tweet => formatTweet(tweet(0)))
	}

	def formatTweet(tweet: Any) = {
       tweet.asInstanceOf[String].replaceAll("[^\u0000-\uFFFF]", "")
    }
    def selectNouns(ds: Dataset[(Seq[String], Seq[String])],sparkSession: SparkSession) : Dataset[Seq[String]] = {
   		import sparkSession.implicits._

    	ds.map(x=> getNouns(x._1,x._2))
    } 
    def getNouns(tokens: Seq[String], pos: Seq[String]) : Seq[String] = {

    	val indexes = pos.zipWithIndex.collect{ case(a,b) if (a == "NN" || a == "NNS") => b}
    	indexes.map(tokens)
    }
}
