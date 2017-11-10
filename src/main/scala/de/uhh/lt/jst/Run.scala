package de.uhh.lt.jst

import de.uhh.lt.jst.dt._

object Run extends BaseRun {

  override val appDescription =
    "A system for word sense induction and disambiguation based on JoBimText approach"

  override val appName = "./jst"

  val jobGroups = List(
    new JobGroup(
      "Convert Commands",
      List(
        CoNLL2DepTermContext,
        Text2TrigramTermContext
      )
    ),
    new JobGroup(
      "Create Commands",
      List(
        // Create Distributional Thesaurus
        WordSimFromCounts, // Deprecated
        WordSimFromTermContext
      )
    ),
    new JobGroup(
      "Filter Commands",
      List(
        // DT Filter
        DTCaseFilter,
        DTFilter,
        // Term Representation filter
        FreqFilter,
        // Count Filter
        WordFeatureFilter
      )
    ),
    new JobGroup(
      "WSD Commands",
      List(
        wsd.Clusters2Features,
        wsd.SenseFeatureAggregator,
        wsd.SenseFeatureFilter,
        wsd.SensesFilter,
        wsd.WSD
      )
    ),
    new JobGroup(
      "Corpus Commands",
      List(
        corpus.CoarsifyPosTags,
        corpus.Conll2Texts,
        corpus.FilterSentences,
        corpus.ReformatConll,
        corpus.StoreToElasticSearch
      )
    ),
    new JobGroup(
      "Miscellaneous Commands",
      List(
        verbs.Conll2Features,
        warc.WarcToDocuments
      )
    )
  )

}

