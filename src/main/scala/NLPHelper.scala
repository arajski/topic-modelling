import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}

object NLPHelper {

	val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

	def processDocuments(documents: DataFrame, column: String) : Dataset[Seq[String]] = {
		val validDocuments = removeInvalidCharacters(documents, column)
		val output = validDocuments.select(lemma(lower('value)).as('words))

		output.toDF("tokens").as[Seq[String]]
	}

	def removeInvalidCharacters(df: DataFrame, column: String) : Dataset[String] = {
		df.select(col(column)).map(tweet => formatTweet(tweet(0)))
	}

	def formatTweet(tweet: Any) = {
       tweet.asInstanceOf[String].replaceAll("[^\u0000-\uFFFF]", "")
    }

}
