name := "josimtext"

version := "0.3"

scalaVersion := "2.10.6"

resolvers += Resolver.mavenLocal

//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.3.0-cdh5.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
