package de.uhh.lt.jst


abstract class Job {

  type ConfigType

  val config: ConfigType

  val appName: String = "jst"

  val command: String = this.getClass.getSimpleName
  val description: String

  val parser: Parser
  def run(config: ConfigType): Unit

  def main(args: Array[String]): Unit = {

    run(parser.parse(args, config).get)
  }

  def printHelp(): Unit = {
    parser.showHeader()
  }

  def checkArgs(args: Array[String]): Boolean = parser.parse(args, config).nonEmpty

  abstract class Parser extends scopt.OptionParser[ConfigType](appName + " " + command) {
    override val showUsageOnError = false
    head(" ")

    note(s"$description\n")
    note("Options:")

    help("help").text("prints this usage text")
  }

}