package de.uhh.lt.jst

// Attention !!!
//
// subclasses have to use ElasticSearchParser instead of the Parser class!
//
abstract class ElasticSearchSparkJob extends SparkJob {


  abstract class ElasticSearchParser extends Parser {
    opt[String]("es-user").
      action { (x, c) => additionalSparkConf.set("spark.es.net.http.auth.user", x); c}.
      required().
      text("User for Elastic Search, default: elasticsearch").
      withFallback(() => "elasticsearch")

    opt[String]("es-pass").
      action { (x, c) => additionalSparkConf.set("spark.es.net.http.auth.pass", x); c}.
      text("Password for Elastic Search").
      required()

    // FIXME defaults is currently defined in conf/defaults.conf, good idea?
    opt[String]("es-nodes").
      action { (x, c) => additionalSparkConf.set("spark.es.nodes", x); c}.
      text("List of Elastic Search nodes, default: localhost")

    // FIXME defaults is currently defined in conf/defaults.conf, good idea?
    opt[Int]("es-batch-size").
      action { (x, c) => additionalSparkConf.set("spark.es.batch.size.bytes", s"${x * 1000000}"); c}.
      valueName("<int>").
      text("Max. size of a batch in MB, default: 1")

    // FIXME defaults is currently defined in conf/defaults.conf, good idea?
    opt[Int]("es-batch-entries").
      action { (x, c) => additionalSparkConf.set("es.batch.size.entries", s"$x"); c}.
      valueName("<int>").
      text("Max. size of a batch in number of documents, default: 1000")
  }
}