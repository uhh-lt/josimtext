package de.uhh.lt.jst.wsd


import com.holdenkarau.spark.testing.SharedSparkContext
import de.uhh.lt.testing.tags.NeedsMissingFiles
import org.scalatest._
import de.uhh.lt.jst.utils.Const

class WSDTest extends FlatSpec with Matchers with SharedSparkContext {

  val wordFeaturesStr = "Python  be  a  widely  use"
  val holingTargetFeaturesStr = "nn(@,interpreter)  nn(@,execution)  #_@_interpreter  allow_@_code"
  val holingAllFeaturesStr = "nn(@,interpreter)  nn(@,execution)  #_@_interpreter  allow_@_code  cop(@,available)  amod(many,@)  amod(@,system)  installation_@_many  on_@_operating  many_@_system"

  "WSD" should "separate deps and trigrams" in {
    val holingFeaturesStr = "punct(emphasize,@)  punct(@,.)  prep_of(line,@)  #_@_design  its_@_philosophy  design_@_emphasize"
    val (deps, trigrams) = WSD.getHolingFeatures(holingFeaturesStr, true)
    deps should contain("punct(emphasize,@)")
    deps should contain("prep_of(line,@)")
    deps should contain("prep_of(@,line)")
    deps should contain("punct(.,@)")
    deps.size should equal(6)

    trigrams.size should equal(3)
    trigrams should contain("design_@_emphasize")
  }

  "Depsall" should "extract context features" in {
    val featuresMode = WSDFeatures.Depsall
    val contextFeatures = WSD.getContextFeatures(wordFeaturesStr, holingTargetFeaturesStr, holingAllFeaturesStr, featuresMode)

    contextFeatures should contain("nn(@,interpreter)")
    contextFeatures should contain("cop(@,available)")
    contextFeatures should contain("cop(available,@)")
    contextFeatures should contain("amod(many,@)")
    contextFeatures should contain("amod(@,many)")
    contextFeatures should not contain ("#_@_interpreter")
  }

  "Trigramall" should "extract context features" in {
    val featuresMode = WSDFeatures.Trigramsall
    val contextFeatures = WSD.getContextFeatures(wordFeaturesStr, holingTargetFeaturesStr, holingAllFeaturesStr, featuresMode)

    contextFeatures should not contain ("be")
    contextFeatures should not contain ("use")
    contextFeatures should not contain ("nn(@,interpreter)")
    contextFeatures should not contain ("cop(@,available)")
    contextFeatures should not contain ("cop(available,@)")
    contextFeatures should not contain ("amod(many,@)")
    contextFeatures should not contain ("amod(@,many)")
    contextFeatures should contain("#_@_interpreter")
    contextFeatures should contain("allow_@_code")
    contextFeatures should contain("many_@_system")
  }

  "Trigramstarget" should "extract context features" in {
    val featuresMode = WSDFeatures.Trigramstarget
    val contextFeatures = WSD.getContextFeatures(wordFeaturesStr, holingTargetFeaturesStr, holingAllFeaturesStr, featuresMode)

    contextFeatures should not contain ("be")
    contextFeatures should not contain ("use")
    contextFeatures should not contain ("nn(@,interpreter)")
    contextFeatures should not contain ("cop(@,available)")
    contextFeatures should not contain ("cop(available,@)")
    contextFeatures should not contain ("amod(many,@)")
    contextFeatures should not contain ("amod(@,many)")
    contextFeatures should contain("#_@_interpreter")
    contextFeatures should contain("allow_@_code")
    contextFeatures should not contain ("many_@_system")
    contextFeatures should not contain ("installation_@_many")
  }


  "DepstargetCoocsClustersTrigramstarget" should "extract context features" in {
    val featuresMode = WSDFeatures.DepstargetCoocsClustersTrigramstarget
    val contextFeatures = WSD.getContextFeatures(wordFeaturesStr, holingTargetFeaturesStr, holingAllFeaturesStr, featuresMode)

    contextFeatures should contain("be")
    contextFeatures should contain("use")
    contextFeatures should contain("nn(@,interpreter)")
    contextFeatures should not contain ("cop(@,available)")
    contextFeatures should not contain ("cop(available,@)")
    contextFeatures should not contain ("amod(many,@)")
    contextFeatures should not contain ("amod(@,many)")
    contextFeatures should contain("#_@_interpreter")
    contextFeatures should contain("allow_@_code")
    contextFeatures should not contain ("many_@_system")
    contextFeatures should not contain ("installation_@_many")
    contextFeatures should not contain ("amod(@,system)")
  }


  "DepstargetCoocsClusters" should "extract context features" in {
    val featuresMode = WSDFeatures.DepstargetCoocsClusters
    val contextFeatures = WSD.getContextFeatures(wordFeaturesStr, holingTargetFeaturesStr, holingAllFeaturesStr, featuresMode)

    contextFeatures should contain("be")
    contextFeatures should contain("use")
    contextFeatures should contain("nn(@,interpreter)")
    contextFeatures should not contain ("cop(@,available)")
    contextFeatures should not contain ("cop(available,@)")
    contextFeatures should not contain ("amod(many,@)")
    contextFeatures should not contain ("amod(@,many)")
    contextFeatures should not contain ("#_@_interpreter")
    contextFeatures should not contain ("allow_@_code")
    contextFeatures should not contain ("many_@_system")
    contextFeatures should not contain ("installation_@_many")
    contextFeatures should not contain ("amod(@,system)")

  }


  "WSD" should "boost features" in {
    val holingFeaturesStr = "punct(emphasize,@)  punct(@,.)  prep_of(line,@)  #_@_design  its_@_philosophy  design_@_emphasize"
    val boosted = WSD.boostFeatures(holingFeaturesStr.split("  ").toList, 2)
    boosted.size should equal(12)
    boosted should contain("punct(emphasize,@)")
  }

  def wsd(mode: WSDFeatures.Value, outputPath: String = "") = {
    val senses = getClass.getResource(Const.PRJ_TEST.SENSES).getPath()
    val output = if (outputPath == "") senses + "-output" else outputPath
    val clusters = Const.PRJ_TEST.WSD_RES.clusters
    val coocs = Const.PRJ_TEST.WSD_RES.coocs
    val trigrams = Const.PRJ_TEST.WSD_RES.trigrams
    val deps = Const.PRJ_TEST.WSD_RES.deps
    val contexts = Const.PRJ_TEST.WSD_RES.contexts

    println(s"Senses: $senses")
    println(s"Output: $output")

    WSD.run(sc, contexts, output, clusters, coocs, deps, trigrams, true, mode, 20000, 1)
  }

  ignore should "run DepstargetCoocsClustersTrigramstarget" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.DepstargetCoocsClustersTrigramstarget)
  }

  ignore should "run DepsallCoocsClustersTrigramsall" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.DepsallCoocsClustersTrigramsall)
  }

  ignore should "run DepsallCoocsClusters" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.DepsallCoocsClusters)
  }

  ignore should "run DepstargetCoocsClusters" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.DepstargetCoocsClusters)
  }

  ignore should "run TrigramstargetDepstarget" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.TrigramstargetDepstarget)
  }

  ignore should "run Depstarget" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.Depstarget)
  }

  ignore should "run Clusters" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.Clusters)
  }

  ignore should "run Coocs" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.Coocs)
  }

  ignore should "run Depsall" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.Depsall)
  }

  ignore should "run Trigramsall" taggedAs NeedsMissingFiles in {
    wsd(WSDFeatures.Trigramsall)
  }

  ignore should "run Trigramstarget" in {
    wsd(WSDFeatures.Trigramstarget)
  }
}