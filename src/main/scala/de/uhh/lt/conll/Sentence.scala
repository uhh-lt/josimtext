package de.uhh.lt.conll

// Using Seq instead of List because of SPARK-16792
case class Sentence(comments: Seq[String], rows: Seq[Row])
