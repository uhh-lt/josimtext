package de.uhh.lt.conll

case class Sentence(deps: Map[Int,Dependency], // a list of parsed dependencies
                    documentID:String = "", // document id
                    sentenceID:Int = -1, // id of the sentence
                    text:String = "") // text of the sences)

