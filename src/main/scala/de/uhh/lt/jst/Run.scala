package de.uhh.lt.jst

import de.uhh.lt.jst.dt._

object Run extends BaseRun {

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
    )
  )

  override val appDescription =
    "A tool to compute term similarities (.i.e a Distributional Thesaurus (DT))"
}

