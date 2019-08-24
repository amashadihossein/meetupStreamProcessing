name := "sbt-spark-sample"

version := "0.1"

scalaVersion := "2.12.8"

// see for info: https://hortonworks.com/tutorial/setting-up-a-spark-development-environment-with-scala/
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3", 
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.3" % "provided", 
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3"
)
