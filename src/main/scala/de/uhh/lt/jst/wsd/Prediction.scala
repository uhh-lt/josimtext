package de.uhh.lt.jst.wsd

import de.uhh.lt.jst.utils.Const

import scala.collection.immutable.Iterable

/**
  * Created by sasha on 13/06/17.
  */
case class Prediction(var confs: List[String] = List(Const.NO_FEATURE_LABEL),
                      var predictConf: Double = Const.NO_FEATURE_CONF,
                      var usedFeaturesNum: Double = Const.NO_FEATURE_CONF,
                      var bestConfNorm: Double = Const.NO_FEATURE_CONF,
                      var usedFeatures: Iterable[String] = List(),
                      var allFeatures: Iterable[String] = List(),
                      var sensePriors: Iterable[String] = List(),
                      var predictRelated: Iterable[String] = List())
