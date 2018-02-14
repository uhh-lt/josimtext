package de.uhh.lt.jst

// Attention !!!
//
// subclasses have to use ElasticSearchParser instead of the Parser class!
//
abstract class ElasticSearchSparkJob extends SparkJob {


  abstract class ElasticSearchParser extends Parser {

    val defaultUser = "elasticsearch"
    opt[String]("es-user").
      action { (x, c) => additionalSparkConf.set("spark.es.net.http.auth.user", x); c}.
      required().
      text(s"User for Elastic Search, default: $defaultUser").
      withFallback(() => defaultUser)

    opt[String]("es-pass").
      action { (x, c) => additionalSparkConf.set("spark.es.net.http.auth.pass", x); c}.
      text("Password for Elastic Search").
      required()

    val defaultNode = "localhost"
    opt[String]("es-nodes").
      action { (x, c) => additionalSparkConf.set("spark.es.nodes", x); c}.
      text(s"List of Elastic Search nodes, default: $defaultNode").
      withFallback(() => defaultNode)

    val defaultSize = 1000000
    val bytesPerMB = 1000000
    opt[Int]("es-batch-size").
      action { (x, c) => additionalSparkConf.set("spark.es.batch.size.bytes", s"${x * bytesPerMB}"); c}.
      valueName("<int>").
      text(s"Max. size of a batch in MB, default: ${defaultSize / bytesPerMB}").
      withFallback(() => defaultSize)

    val defaultEntries = 1000
    opt[Int]("es-batch-entries").
      action { (x, c) => additionalSparkConf.set("es.batch.size.entries", s"$x"); c}.
      valueName("<int>").
      text(s"Max. size of a batch in number of documents, default: $defaultEntries").
      withFallback(() => defaultEntries)
  }
}