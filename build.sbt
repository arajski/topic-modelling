name := "TopicModelling"
 
version := "1.0"
 
scalaVersion := "2.11.0"
sparkVersion := "2.1.0"

sparkComponents ++= Seq(
	"core", "sql", "mllib"
)


libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

logBuffered in Test := false
parallelExecution in Test := false