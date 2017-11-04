package de.uhh.lt.jst


trait BaseRun {
  type JobGroup = (String, List[Job])
  val jobGroups: List[JobGroup]
  val appName: String
  val appDescription: String

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case None => printHelp()
      case Some(command) => runCommand(command, args.drop(1))
    }
  }

  private def allJobs = jobGroups.flatMap(_._2)

  def runCommand(command: String, args: Array[String]): Unit = {

    allJobs.find( _.command == command ) match {
      case None => println(s"$appName: '$command' is not a command.")
      case Some(job: Job) => if (job.checkArgs(args)) job.main(args) else job.printHelp()
    }
  }

  def printHelp(): Unit = {
    println(s"\nUsage: $appName COMMAND\n")
    println(s"$appDescription\n")

    val maxLength = allJobs.map(_.command.length).max

    jobGroups.foreach { case (name, jobs) =>
      println(s"$name:")
      jobs.foreach { j => println(s" ${j.command.padTo(maxLength, ' ')}   ${j.description}") }
      println("")
    }

    println(s"Run '$appName COMMAND --help' for more information on a command.")
  }
}
