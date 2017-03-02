// import required spark classes
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

 
// define main method (scala entry point)
object TopicModelling {
  def main(args: Array[String]) {
 
    // initialise spark context
    val conf = new SparkConf().setAppName("TopicModelling")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile("hdfs://localhost:9000/hdfs/data/Oct21.txt")
    val counts = textFile.flatMap(line => line.split(" "))
                                   
    println("RESULT:")
    println(counts.collect().mkString(", ")) 
    println("=========")
    // terminate spark context
    sc.stop()
    
  }
}

