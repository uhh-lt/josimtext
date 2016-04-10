import scala.util.Try

object WSDFeatures extends Enumeration {
    type WSDFeatures = Value
    val Depstarget, Depsall, Coocs, Clusters, DepsallCoocsClusters, DepstargetCoocsClusters,
        DepstargetCoocsClustersTrigramstarget, DepsallCoocsClustersTrigramsall, Trigramsall,
        Trigramstarget, TrigramstargetDepstarget = Value

    def trigramsNeeded(v: WSDFeatures): Boolean = v.toString.toLowerCase contains "trigram"

    def trigramsallNeeded(v: WSDFeatures): Boolean = v.toString.toLowerCase contains "trigramsall"

    def trigramstargetNeeded(v: WSDFeatures): Boolean = v.toString.toLowerCase contains "trigram"

    def depsallNeeded(v: WSDFeatures): Boolean = v.toString.toLowerCase contains "depsall"

    def depstargetNeeded(v: WSDFeatures): Boolean = v.toString.toLowerCase contains "deps"

    def depsNeeded(v: WSDFeatures): Boolean = depstargetNeeded(v)

    def coocsNeeded(v: WSDFeatures): Boolean = v.toString.toLowerCase contains "coocs"

    def clustersNeeded(v: WSDFeatures): Boolean = v.toString.toLowerCase contains "cluster"

    def wordsNeeded(v: WSDFeatures): Boolean = {
        val vstr = v.toString.toLowerCase
        (vstr contains "cooc") || (vstr contains "cluster")
    }

    def fromString(str:String) = {
        val res1 = Try(WSDFeatures.withName(str))
        if (!res1.isSuccess) {
            val res2 = Try(WSDFeatures.withName(str(0).isUpper + str.substring(1).toLowerCase()))
            if (!res2.isSuccess) {
                DepstargetCoocsClusters
            } else {
                res2.getOrElse(DepstargetCoocsClusters)
            }
        } else {
            res1.getOrElse(DepstargetCoocsClusters)
        }
    }
}