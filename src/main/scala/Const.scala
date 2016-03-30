
object Const {
  val LIST_SEP = "  "
  val SCORE_SEP = ':'
  val HOLE = "@"
  val HOLE_DEPRECATED = "@@"
  val NO_FEATURE_LABEL = "-1"
  val NO_FEATURE_CONF = 0.0
  val PRIOR_PROB = 0.00001

  object Resources{
      val STOPWORDS = "/stoplist_en.csv"
      val STOP_DEPENDENCIES = Set("dep", "punct", "cc", "possessive")
  }

  object PRJ {
      val FEATURES = "/prj-f.csv"
      val SENSES = "/prj-senses.csv"
      val WORDS = "/prj-w.csv"
      val WORD_FEATURES = "/prj-wf.csv"
      val RES_DIR = "/Users/alex/Desktop/prj/test-res"
  }
}
