name := "SensorHumidityProcessor"

version := "1.0"

scalaVersion := "2.13.14" // Scala version

resolvers ++= Seq(
  "Alpakka Repo" at "https://repo.alpakka.io/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.13" % "3.5.3",
  "org.apache.spark" % "spark-sql_2.13" % "3.5.3",
  "org.scala-lang" % "scala-library" % "2.13.14",
  "org.scalatest" % "scalatest_2.13" % "3.2.19",
  "com.holdenkarau" % "spark-testing-base_2.13" % "3.5.3_2.0.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "git.properties") => MergeStrategy.discard
  case PathList("META-INF", "module-info.class") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers += "Typesafe Repo" at "https://repo.typesafe.com/typesafe/repo"

//spark-submit --verbose --class SensorHumidityProcessor target/scala-2.13/sensorhumidityprocessor_2.13-1.0.jar /Users/vigneshprasad96/Documents/GitHub/sensor-processing/sensorData
//sbt "run /Users/vigneshprasad96/Documents/GitHub/sensor-processing/sensorData" 