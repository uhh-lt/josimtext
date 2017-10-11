package de.uhh.lt.jst

abstract class Job {

  type Config
  val config: Config

  val appName: String

  val command: String = this.getClass.getSimpleName
  val description: String


  val parser: Parser
  def run(config: Config): Unit

  def main(args: Array[String]): Unit = {
    run(parser.parse(args, config).get)
  }

  def printHelp(): Unit = {
    parser.showHeader()
  }

  def checkArgs(args: Array[String]): Boolean = parser.parse(args, config).nonEmpty

  abstract class Parser extends scopt.OptionParser[Config](appName + " " + command) {
    override val showUsageOnError = false

    note(s"$description\n")
    note("Options:")

    help("help").text("prints this usage text")
  }

}