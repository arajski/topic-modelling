import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.scalatest.Assertions._
import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.functions._

class NLPHelperTest extends FunSuite with BeforeAndAfter{

  private val master = "local[2]"
  private val appName = "NLPTest"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  test("it should retrieve hours and dates") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("14:00 AM 1st January 2017")).toDF("dates")
    val output = NLPHelper.processDocuments(input,"dates")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("14:00", "AM", "1st", "January", "2017")

    assert(result == expected)
  }

  test("it tokenizes a simple tweet document") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("This is a test Tweet, http://test.url #hashtag#secondhashtag @user.")).toDF("tweet")
    val output = NLPHelper.processDocuments(input,"tweet")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("this", "be", "a", "test", "Tweet",",","http://test.url", "#hashtag","#secondhashtag", "@user", ".")

    assert(result == expected)
  }

  test("it should use lemmatization") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("I am. You are. You shouldn't have done it so quickly.")).toDF("lemmas")
    val output = NLPHelper.processDocuments(input,"lemmas")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("I", "be", ".", "you", "be", ".", "you", "should", "not", "have", "do", "it", "so", "quickly", ".")

    assert(result == expected)
  }

  test("it should return dataset") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("Test text")).toDF("text")
    val result = NLPHelper.processDocuments(input,"text")

    assert(result.getClass.getName == "org.apache.spark.sql.Dataset")
  }

  test("it should handle multiple documents") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("First document"),("Second documents"), ("Third document")).toDF("documents")
    val result = NLPHelper.processDocuments(input,"documents")

    assert(result.count == 3)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
  }
}