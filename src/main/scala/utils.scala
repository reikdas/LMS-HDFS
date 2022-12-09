trait ArgParser extends FileOps {

  def parseargs(args: Array[String]) = {
    implicit class RegexOps(sc: StringContext) {
      def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }

    val options: Map[String, Any] = args.toList.foldLeft(Map[String, Any]()) {
      case (options, r"--loadFile=(\/\w+.txt)$e") => options + ("loadFile" -> e)
      case (options, r"--writeFile=(\w+.c)$e") => options + ("writeFile" -> e)
      case (options, "--bench") => options + ("bench" -> true)
      case (options, "--print") => options + ("print" -> true)
      case (options, "--mmap") => options + ("mmap" -> true)
    }

    options
  }
}
