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

    val input = Seq(("14:00 PM o'clock 1st January 2017")).toDF("text")
    val output = NLPHelper.processDocuments(input,"text")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("14:00", "pm", "o'clock", "1st", "january", "2017")

    assert(result == expected)
  }

  test("it tokenizes a simple tweet document") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("This is a test Tweet, \"test cite\" http://test.url #hashtag#secondhashtag @user.!!!")).toDF("text")
    val output = NLPHelper.processDocuments(input,"text")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("this", "be", "a", "test","tweet", ",","``","test","cite","''","http://test.url", "#hashtag","#secondhashtag", "@user", ".","!!!")

    assert(result == expected)
  }

  test("it should use lemmatization") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("I am. You are. You shouldn't have done it so quickly.")).toDF("text")
    val output = NLPHelper.processDocuments(input,"text")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("i", "be", ".", "you", "be", ".", "you", "should", "not", "have", "do", "it", "so", "quickly", ".")

    assert(result == expected)
  }
  test("it should remove invalid characters") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("This is an emoji test�.Second���test")).toDF("text")
    val output = NLPHelper.processDocuments(input,"text")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("this", "be", "a", "emoji", "test",".","second","test")

    assert(result == expected)
  }
  test("it should remove emojis") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val tweet = Seq("Test tweet😄😃").toDF("text")
    val result = NLPHelper.removeInvalidCharacters(tweet,"text").first()
    assert(result == "Test tweet")
  }
  test("it returns tokens in lowercase") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("lowercase UPPERCASE Standard CaMeLcAsE")).toDF("text")
    val output = NLPHelper.processDocuments(input,"text")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("lowercase", "uppercase", "standard", "camelcase")

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

    val input = Seq(("First document"),("Second documents"), ("Third document")).toDF("text")
    val result = NLPHelper.processDocuments(input,"text")

    assert(result.count == 3)
  }
  test("it should tokenize brackets") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq(("(test) with {brackets} and [stuff]")).toDF("text")
    val output = NLPHelper.processDocuments(input,"text")
    val result = output.select(col("tokens")).first().get(0)
    val expected = Seq("-lrb-", "test", "-rrb-", "with", 
      "-lcb-", "bracket", "-rcb-", "and", 
      "-lsb-", "stuff", "-rsb-")

    assert(result == expected)
  }
  test("it should filter nouns") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val input = Seq((Seq("apple","house","writing","with","happy"),Seq("NNS","NN", "ADV", "VER", "ADV"))).toDF("tokens","pos").as[(Seq[String], Seq[String])]
    val output = NLPHelper.selectNouns(input)
    val result = output.select("value").first().get(0)
    val expected = Seq("apple","house")

    assert(result == expected)
  }
  after {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
  }
}