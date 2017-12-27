package de.uhh.lt.jst

abstract class ElasticSearchSparkJob extends SparkJob {

  // Attention: subclasses have to use ElasticSearchParser instead of the Parser class!
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

    // FIXME defaults is currently defined in conf/defaults.conf as 'localhost'
    opt[String]("es-nodes").
      action { (x, c) => additionalSparkConf.set("spark.es.nodes", x); c}.
      text("List of Elastic Search nodes, default localhost")
  }
}