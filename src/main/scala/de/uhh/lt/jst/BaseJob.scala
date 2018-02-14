package de.uhh.lt.jst

import java.io.{File, FileInputStream}
import java.util.Properties
import collection.JavaConverters._

abstract class BaseJob {
  var debug: Boolean = false

  type ConfigType
  val config: ConfigType
  val appName: String = "josimtext"

  // Remove trailing $ from companion objects, .i.e. from MyObject$
  val command: String = this.getClass.getSimpleName.replace('$',' ').trim
  val description: String
  protected val parser: _BaseParser
  def run(config: ConfigType): Unit

  def main(args: Array[String]): Unit = {
    run(parser.parse(args, config).get)
  }

  def printHelp(): Unit = {
    parser.showHeader()
  }

  def parseArgs(args: Array[String]): Option[ConfigType] = parser.parse(args, config)

  abstract class _BaseParser extends scopt.OptionParser[ConfigType](appName + " " + command) {
    override val showUsageOnError = false
    head(" ")
    note(s"$description\n")
    note("Options:")

    help("help").text("prints this usage text")

    opt[File]("properties-file").
      text(s"Path to a file from which to load extra properties. Default: conf/defaults.conf").
      action { (x, c) =>
        val props = new Properties()
        props.load(new FileInputStream(x))
        for (key <- props.stringPropertyNames().asScala) {
          System.getProperties.setProperty(key, props.getProperty(key))
        }
        c
      }.
      withFallback(() => new File("conf/defaults.conf")).
      valueName("<file>")

    opt[Unit]("debug").
      hidden().
      action { (x, c) => debug = true; c}
  }

}