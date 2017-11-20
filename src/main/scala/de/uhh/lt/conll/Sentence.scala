package de.uhh.lt.conll

// Using Seq instead of List because of SPARK-16792
case class Sentence(deps: Seq[Dependency],
                    documentID:String = "",
                    sentenceID:Int = -1,
                    text:String = "")

case class SentenceOld(deps: Seq[Dependency])
