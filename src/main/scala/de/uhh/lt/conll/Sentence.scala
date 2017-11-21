package de.uhh.lt.conll

case class Sentence(deps: Map[Int,Dependency],
                    documentID:String = "",
                    sentenceID:Int = -1,
                    text:String = "")
