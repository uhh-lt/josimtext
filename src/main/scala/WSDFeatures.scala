import scala.util.Try

object WSDFeatures extends Enumeration {
  type WSDFeatures = Value
  val DepsTarget, Coocs, Clusters, Deps, DepsCoocsClusters, DepsAllCoocsClusters = Value

  val DEFAULT = WSDFeatures.DepsCoocsClusters

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