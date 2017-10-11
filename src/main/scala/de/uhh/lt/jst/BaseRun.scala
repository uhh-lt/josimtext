package de.uhh.lt.jst


trait BaseRun {
  val jobs: List[Job]
  val appName: String
  val appDescription: String

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case None => printHelp()
      case Some(command) => runCommand(command, args.drop(1))
    }
  }

  def runCommand(command: String, args: Array[String]): Unit = {
    jobs.find( _.command == command ) match {
      case None => println(s"$appName: '$command' is not a command.")
      case Some(job: Job) => if (job.checkArgs(args)) job.main(args) else job.printHelp()
    }
  }

  def printHelp(): Unit = {
    println(s"\nUsage: $appName COMMAND\n")
    println(s"$appDescription\n") // TODO
    println("Commands:")
    val maxLength = jobs.map(_.command.length).max
    jobs.foreach { j =>
      println(s" ${j.command.padTo(maxLength, ' ')}   ${j.description}")
    }
    println(s"\nRun '$appName COMMAND --help' for more information on a command.")
  }
}
