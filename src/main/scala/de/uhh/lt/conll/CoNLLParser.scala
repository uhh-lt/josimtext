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
    // refactor using filter and split("\n")
    val lines = conllSentence.lines.buffered
    val isComment = (s: String) => s.startsWith("#")
    val isNotComment = (s: String) => !isComment(s)
    val commentLines: Seq[String] = takeFromIteratorWhile(lines, isComment)
    val depLines = takeFromIteratorWhile(lines, isNotComment)

    var documentID = ""
    var sentenceID = 0
    val sentID: Seq[String] = commentLines.filter{_.startsWith("# sent_id = ")}
    if(sentID.length > 0) {
      val fields = sentID(0)
        .substring(SENTID_PREFIX.length)
        .split(DOCID_SENT_ID_SEP)
      if (fields.length >= 2) {
        documentID = fields(0) // FIX: all but last
        sentenceID = fields.last.toInt
      }
    }

    var sentenceText = ""
    val textLines: Seq[String] = commentLines.filter{_.startsWith(TEXT_PREFIX)}
    if(textLines.length > 0) {
      sentenceText = textLines(0).substring(TEXT_PREFIX.length)
    }

    // Split without -1 would remove empty values: https://stackoverflow.com/a/14602089
    val deps = depLines
      .map{_.split("\t", -1)}
      .filter{ line => line.length == NUM_COLUMNS}
      .map{readDependency}

    Sentence(deps=deps, documentID, sentenceID, sentenceText)
  }

  private def readDependency(fields: Array[String]): Dependency = {
    if (fields.length != NUM_COLUMNS) {
      throw CoNLLSchemaError(s"Row has not 10 columns but ${fields.length}, content: ${fields.mkString("-t-")}")
    }

    Dependency(
      id = fields(0),
      form = fields(1),
      lemma = fields(2),
      upostag = fields(3),
      xpostag = fields(4),
      feats = fields(5),
      head = fields(6),
      deprel = fields(7),
      deps = fields(8),
      misc = fields(9)
    )
  }

  private def takeFromIteratorWhile(iter: BufferedIterator[String], predicate: (String) => Boolean) = {
    var result = List[String]()
    while(iter.hasNext && predicate(iter.head))
      result = result ::: iter.next() :: Nil
    result
  }

}
