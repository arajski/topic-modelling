import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.scalatest.Assertions._
import org.scalatest.FunSuite
import scala.collection.mutable.Stack
import org.scalatest.BeforeAndAfter

class StopWordsHelperTest extends FunSuite with BeforeAndAfter{

  private val master = "local[2]"
  private val appName = "StopWordsTest"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)

  }

  test("it should contain list of stopwords") {
    var stopWords = StopWordsHelper.getStopWords()

    assert(stopWords.size > 0)
  }

  test("it should remove stopwords from passed dataset") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._
    
    val words = Seq("i", "am", "a", "president", "donald", "trump")
    val result = StopWordsHelper.removeStopWords(words)
    val expected = Seq("president")

    assert(result == expected)
  }
  test("it should remove special tokens") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._
    
    val words = Seq("a", "@test", "of", "special", "#tokens", "http://google.com", "14:00","...","!","......")
    val result = StopWordsHelper.removeStopWords(words)
    val expected = Seq("special","14:00")

    assert(result == expected)
  }

  test("it should return a Sequence") {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    val words = Seq("i", "am", "a", "president", "donald", "trump")
    val result = StopWordsHelper.removeStopWords(words)
    
    assert(result.getClass.getName == "scala.collection.immutable.$colon$colon")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    System.clearProperty("spark.driver.port")
  }
}