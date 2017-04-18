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

		val output = documents
		  .select(lemma(lower(col(column))).as('words))

		return output.toDF("tokens").as[Seq[String]]
	}

}
