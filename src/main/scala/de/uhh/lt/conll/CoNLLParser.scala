package de.uhh.lt.conll
import scala.util.{Try,Success,Failure}

object CoNLLParser {

  val NUM_COLUMNS = 10
  val TEXT_PREFIX = "# text = "
  val SENTID_PREFIX = "# sent_id = "
  //val NEWDOCID_PREFIX = "# newdoc"
  val DOCID_SENT_ID_SEP = "#" // e.g. like in 'http://goo.gl/page/#1' where '1' is sentence id

  case class CoNLLSchemaError(msg: String) extends Exception(msg)

  def parseSingleSentence(conllSentence: String): Sentence = {
    val lines = conllSentence.split("\n")

    // Find sentence ID and document ID
    val sentID = lines.filter(_.startsWith(SENTID_PREFIX))
    var documentID = ""
    var sentenceID = 0
    if(sentID.length > 0) {
      val fields = sentID(0)
        .substring(SENTID_PREFIX.length)
        .split(DOCID_SENT_ID_SEP)
      if (fields.length >= 2) {
        documentID = fields.init.mkString("/")
        sentenceID = fields.last.toInt
      }
    }

    // Find sentence text
    var sentenceText = ""
    val textLines = lines.filter(_.startsWith(TEXT_PREFIX))
    if(textLines.length > 0) {
      sentenceText = textLines(0).substring(TEXT_PREFIX.length)
    }

    // Find dependencies
    val deps = lines
      .filter{ line => !line.startsWith("#") && line.length >= NUM_COLUMNS }
      .map{ _.split("\t", -1) } // Split without -1 would remove empty values: https://stackoverflow.com/a/14602089
      .filter{ line => line.length == NUM_COLUMNS}
      .map{ createDependency }

    val depsMap = deps
      .map(_.id)
      .zip(deps)
      .toMap

    Sentence(depsMap, documentID, sentenceID, sentenceText)
  }

  private def createDependency(fields: Array[String]): Dependency = {
    if (fields.length != NUM_COLUMNS) {
      throw CoNLLSchemaError(s"Row has not 10 columns but ${fields.length}, content: ${fields.mkString("-t-")}")
    }

    Dependency(
      id = fields(0).toInt,
      form = fields(1),
      lemma = fields(2),
      upostag = fields(3),
      xpostag = fields(4),
      feats = fields(5),
      head = fields(6).toInt,
      deprel = fields(7),
      deps = fields(8),
      ner = fields(9)
    )
  }
}
