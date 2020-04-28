name := "LendingClub"

version := "0.1"

scalaVersion := Option(System.getProperty("scala.version")).getOrElse("2.11.12")


scalacOptions ++= Seq("-unchecked", "-deprecation")


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}