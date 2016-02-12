import scala.util.Try

object WSDFeatures extends Enumeration {
  type WSDFeatures = Value
  val Depstarget, Depsall, Coocs, Clusters, DepstargetCoocsClusters, DepsallCoocsClusters = Value
  val DEFAULT = WSDFeatures.DepstargetCoocsClusters

  def wordsNeeded(wsdFeatures: WSDFeatures): Boolean = {
      (wsdFeatures == Coocs || wsdFeatures == Clusters || wsdFeatures == DepstargetCoocsClusters || wsdFeatures == DepsallCoocsClusters)
  }

  def depsallNeeded(wsdFeatures: WSDFeatures): Boolean = {
      (wsdFeatures == Depsall || wsdFeatures == DepsallCoocsClusters)
  }

  def depstargetNeeded(wsdFeatures: WSDFeatures): Boolean = {
      (wsdFeatures == Depstarget || wsdFeatures == DepstargetCoocsClusters || wsdFeatures == Depsall || wsdFeatures == DepsallCoocsClusters)
  }

  def depsNeeded(wsdFeatures: WSDFeatures): Boolean = {
    (wsdFeatures == Depsall || wsdFeatures == DepsallCoocsClusters || wsdFeatures == Depstarget || wsdFeatures == DepstargetCoocsClusters)
  }

  def coocsNeeded(wsdFeatures: WSDFeatures): Boolean = {
    (wsdFeatures == Coocs || wsdFeatures == DepsallCoocsClusters || wsdFeatures == DepstargetCoocsClusters)
  }

  def clustersNeeded(wsdFeatures: WSDFeatures): Boolean = {
    (wsdFeatures == Clusters || wsdFeatures == DepsallCoocsClusters || wsdFeatures == DepstargetCoocsClusters)
  }

  def fromString(str:String) = {
    val res1 = Try(WSDFeatures.withName(str))
    if (!res1.isSuccess) {
      val res2 = Try(WSDFeatures.withName(str(0).isUpper + str.substring(1).toLowerCase()))
      if (!res2.isSuccess) {
        DEFAULT
      } else {
        res2.getOrElse(DEFAULT)
      }
    } else {
      res1.getOrElse(DEFAULT)
    }
  }
}