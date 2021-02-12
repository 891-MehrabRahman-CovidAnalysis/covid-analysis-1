name := "HashtagByRegion"
version := "0.1"
scalaVersion := "2.12.13"

libraryDependencies  ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
  // "com.amazonaws" % "aws-java-sdk" % "1.11.948",         // Uncomment this dependency if running locally
  // "org.apache.hadoop" % "hadoop-aws" % "2.8.5",          // Uncomment this dependency if running locally
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "com.outr" % "scribe-slf4j_2.12" % "3.3.1",
  "com.outr" % "scribe-file_2.12" % "3.3.1"
)

parallelExecution in Test := false
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-fDW", "logs/test/test_results.log", "-oO")

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}