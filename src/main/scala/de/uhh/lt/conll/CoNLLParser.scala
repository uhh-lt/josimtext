package de.uhh.lt.conll

object CoNLLParser {

  case class CoNLLSchemaError(msg: String) extends Exception(msg)

  def parseSingleSentence(text: String): Sentence = {
    val lines = text.lines.buffered
    val isComment = (s: String) => s.startsWith("#")
    val isNotComment = (s: String) => !isComment(s)

    val commentLines = takeFromIteratorWhile(lines, isComment)
    val sentenceLines = takeFromIteratorWhile(lines, isNotComment)

    // Split without -1 would remove empty values: https://stackoverflow.com/a/14602089
    val rows = sentenceLines.map(_.split("\t", -1)).map(readRow)

    Sentence(comments = commentLines, rows = rows)
  }

  private def readRow(row: Array[String]): Row = {
    if (row.length != 10)
      throw CoNLLSchemaError(
        s"Row has not 10 columns but ${row.length}, content: ${row.mkString("-t-")}"
      )

    Row(
      id = row(0),
      form = row(1),
      lemma = row(2),
      upostag = row(3),
      xpostag = row(4),
      feats = row(5),
      head = row(6),
      deprel = row(7),
      deps = row(8),
      misc = row(9)
    )
  }

  private def takeFromIteratorWhile(iter: BufferedIterator[String], predicate: (String) => Boolean) = {
    var result = List[String]()
    while(iter.hasNext && predicate(iter.head))
      result = result ::: iter.next() :: Nil
    result
  }

}
