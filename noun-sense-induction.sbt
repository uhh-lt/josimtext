name := "josimtext"

version := "0.2"

scalaVersion := "2.10.4"

resolvers += Resolver.mavenLocal

//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.3.0-cdh5.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
