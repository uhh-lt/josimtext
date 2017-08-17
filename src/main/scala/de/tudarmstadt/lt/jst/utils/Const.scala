package de.tudarmstadt.lt.jst.utils

object Const {
  val LIST_SEP = "  "
  val SCORE_SEP = ':'
  val POS_SEP = "#"
  val SENSE_SEP = "#"
  val HOLE = "@"
  val HOLE_DEPRECATED = "@@"
  val NO_FEATURE_LABEL = "-1"
  val NO_FEATURE_CONF = 0.0
  val PRIOR_PROB = 0.000010

  object FeatureExtractionTests {
    val conll = "/conll.csv.gz"
    val warc = "/cc-test.warc.gz"
  }

  object Resources{
      val STOPWORDS = "/stoplist_en.csv"
      val STOP_DEPENDENCIES = Set("dep", "punct", "cc", "possessive")
  }

  object PRJ_TEST {
      val FEATURES = "/prj-f.csv"
      val SENSES = "/prj-senses.csv"
      val WORDS = "/prj-w.csv"
      val WORD_FEATURES = "/prj-wf.csv"

      object WSD_RES {
          val dir = "/Users/alex/Desktop/prj/test-res"  // can be downloaded from http://panchenko.me/data/joint/jst/test-res.tar.gz
          val clusters = dir + "/clusters.csv"
          val coocs = dir + "/depwords.csv"
          val trigrams = dir + "/trigrams.csv"
          val deps = dir + "/deps.csv"
          val contexts = dir + "/contexts.csv"
          val wordsTrigram = dir + "/W-trigram"
          val featuresTrigram = dir + "/F-trigram"
          val wordsFeaturesTrigram = dir + "/WF-trigram"
      }
  }
}
