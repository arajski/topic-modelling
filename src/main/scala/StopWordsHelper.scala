import scala.io.Source
import java.io.InputStream
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.Dataset

object StopWordsHelper {
	private val stream : InputStream = getClass.getResourceAsStream("/stopwords.txt")
	private val stopWords = Source.fromInputStream( stream ).getLines.toSet

    def printStopWords() {
        for (elem <- stopWords) println(elem) 
    }
    /**
    * Remove hashtags, user annotations, urls and words smaller than 3 characters, except numbers 
    */
    def isSpecialToken(token: String) : Boolean = {
    	val isSpecial = (token contains "@") || 
    					(token contains "#") || 
    					(token contains "http") ||
    					(token contains "!") ||
    					(token contains ".") ||
    					(token contains "?") ||    
    					((token.length < 3) && Try(token.toDouble).isFailure)
    	isSpecial
    }

    def getStopWords() : Set[String] =
    	stopWords

    def removeStopWords(tokens: Seq[String]) : Seq[String] = 
      	tokens.filter(s => !stopWords.contains(s) && !isSpecialToken(s))
    
}


