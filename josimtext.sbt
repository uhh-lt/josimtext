name := "josimtext"

version := "0.4"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
