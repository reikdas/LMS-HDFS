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
      buf.free
    }
    mpi_finalize()
    if (benchFlag) {
      val end = mpi_wtime()
      printf("Proc %d spent %lf time.\n", world_rank, end - start)
    }
  }
}

object Whitespace {

  def main(args: Array[String]): Unit = {
    val driver = new DslDriverC[Int, Unit] with ArgParser with WhitespaceOps {
      q =>
      override val codegen = new DslGenC with CCodeGenLibFunction with CCodeGenMPI with CCodeGenCMacro with CCodeGenScannerOps {
        registerHeader("<ctype.h>")
        registerHeader("src/main/resources/headers", "\"ht.h\"")
        val IR: q.type = q
      }

      val (loadFile, writeFile, readFunc, benchFlag, printFlag) = parseargs(args)

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val paths = GetPaths(loadFile)
        HDFSExec(paths, readFunc, benchFlag, printFlag)
        paths.free
      }

      def emitMyCode() = {
        codegen.emitSource[Int, Unit](wrapper, "Snippet", new java.io.PrintStream(writeFile))
      }
    }
    driver.emitMyCode()
  }
}
