import lms.core.Backend.Node
import lms.core.stub.{Adapter, DslDriverC, DslGenC}
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{CCodeGenCMacro, CCodeGenLibFunction, CCodeGenMPI, CCodeGenScannerOps}

@virtualize
trait WhitespaceOps extends HDFSOps with FileOps with MyMPIOps with CharArrayOps {

  def HDFSExec(paths: Rep[Array[String]], readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char], benchFlag: Boolean = false, printFlag: Boolean = true) = {
    // MPI initialize
    var world_size = 0
    var world_rank = 0

    val start = mpi_wtime()
    mpi_init()
    mpi_comm_size(mpi_comm_world, world_size)
    mpi_comm_rank(mpi_comm_world, world_rank)

    if (world_rank < paths.length) {
      val buf = NewLongArray[Char](GetBlockLen() + 1, Some(0)) // Buffer to hold characters in file

      var count = 0L
      for (i <- 0 until paths.length) {
        if (i % world_size == world_rank) {

          // Get buffer of chars from file
          val block_num = open(paths(i))
          val size = filelen(block_num)
          val fpointer = mmapFile(block_num, buf, size)

          for (j <- 0 until fpointer.length) {
            if (fpointer(j) == ' ') {
              count = count + 1L
            }
          }
          close(block_num)
        }
      }
      val total_count: Var[Long] = var_new(0L)
      mpi_reduce(count, total_count, 1, mpi_long, mpi_sum, 0, mpi_comm_world)

      if (printFlag) {
        if (world_rank == 0) {
          printf("%ld\n", total_count)
        }
      }
//      else {
//        Adapter.g.reflectWrite("printflag", Unwrap(total_count)(Adapter.CTRL)
//      }
      buf.free
    }
    mpi_finalize()
    if (benchFlag) {
      val end = mpi_wtime()
      printf("Proc %d spent %lf time.\n", world_rank, end - start)
    }
  }
}

object Whitespace extends WhitespaceOps {

  def main(args: Array[String]): Unit = {

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

    val loadFile = options.getOrElse("loadFile", throw new RuntimeException("No load file")).toString
    val writeFile = options.getOrElse("writeFile", throw new RuntimeException("No write file")).toString
    val benchFlag: Boolean = if (options.exists(_._1 == "bench")) { options("bench").toString.toBoolean } else { false }
    val printFlag: Boolean = if (options.exists(_._1 == "print")) { options("print").toString.toBoolean } else { false }
    val readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char] = if (options.exists(_._1 == "mmap")) {
      mmapFile
    } else {
      readFile
    }

    val driver = new DslDriverC[Int, Unit] {
      q =>
      override val codegen = new DslGenC with CCodeGenLibFunction with CCodeGenMPI with CCodeGenCMacro with CCodeGenScannerOps {

        override def traverse(n: Node): Unit = n match {
          case n @ Node(_, "printflag", _, _) =>
          case _ => super.traverse(n)
        }

        registerHeader("<ctype.h>")
        registerHeader("src/main/resources/headers", "\"ht.h\"")
        val IR: q.type = q
      }

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val paths = GetPaths(loadFile)
        val res = HDFSExec(paths, readFunc, benchFlag, printFlag)
        paths.free
      }

      def emitMyCode(path: String) = {
        codegen.emitSource[Int, Unit](wrapper, "Snippet", new java.io.PrintStream(path))
      }
    }
    driver.emitMyCode(writeFile)
  }
}