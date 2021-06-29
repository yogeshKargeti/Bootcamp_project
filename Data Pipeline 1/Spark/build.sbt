import sbt._
import Keys._

version := "0.1"
scalaVersion := "2.9.2"

name := "untitled"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies +=  "mysql" % "mysql-connector-java" % "5.1.46"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"

mainClass in assembly := Some("FinalFile")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
