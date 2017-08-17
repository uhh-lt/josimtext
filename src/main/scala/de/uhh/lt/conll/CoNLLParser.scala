package de.uhh.lt.conll

import java.io.{Reader, StringReader}

import com.univocity.parsers.csv._

object CoNLLParser {
  private val parser: CsvParser = {
    val NULL_CHAR = '\u0000'
    val quoteChar: Char = NULL_CHAR
    val escapeChar: Char = NULL_CHAR
    val delimiter: Char = '\t'
    val lineSeparator: String = "\n" // ?

    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(delimiter)
    format.setQuote(quoteChar)
    format.setQuoteEscape(escapeChar)
    format.setLineSeparator(lineSeparator)
    //format.setComment(NULL_CHAR) ?
    //settings.setIgnoreLeadingWhitespaces(true) ?
    //settings.setIgnoreTrailingWhitespaces(true) ?
    settings.setReadInputOnSeparateThread(false)
    //settings.setInputBufferSize(128) ?
    //settings.setMaxColumns(20480) ?
    settings.setNullValue("")
    //settings.setMaxCharsPerColumn(-1) ?
    //settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_DELIMITER)

    new CsvParser(settings)
  }

  def parseSingleSentence(text: String): Sentence = {
    val lines = text.lines.buffered
    val isComment = (s: String) => s.startsWith("#")
    val isNotComment = (s: String) => !isComment(s)

    val commentLines = takeFromIteratorWhile(lines, isComment)
    val sentenceLines = takeFromIteratorWhile(lines, isNotComment)

    val rows = sentenceLines.map(parser.parseLine).map(readRow)

    Sentence(comments = commentLines, rows = rows)
  }

  private def readRow(row: Array[String]): Row = {
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
