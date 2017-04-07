import scala.io.Source
import java.io.InputStream
import org.apache.spark.sql.Dataset

object StopWordsHelper {
	private val stream : InputStream = getClass.getResourceAsStream("/stopwords.txt")
	private val stopWords = Source.fromInputStream( stream ).getLines.toSet

    def printStopWords() {
        for (elem <- stopWords) println(elem) 
    }

    def getStopWords() : Set[String] =
    	stopWords

    def removeStopWords(ds: Dataset[String]) : Dataset[String] = 
      	ds.filter(s => !stopWords.contains(s))
    
}


