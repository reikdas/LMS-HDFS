import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize

@virtualize
class WhitespaceOps extends DDLoader {

  def HDFSExec(paths: Rep[Array[String]], readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char], benchFlag: Boolean = false, printFlag: Boolean = true) = {
    // MPI initialize
    var world_size = 0
    var world_rank = 0

    val start = timestamp
    Adapter.g.reflectWrite("printflag", Unwrap(start))(Adapter.CTRL)
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
      val end = timestamp
      Adapter.g.reflectWrite("printflag", Unwrap(end))(Adapter.CTRL)
      printf("Proc %d spent %ld time.\n", world_rank, end - start)
    }
    paths.free
  }
}

object Whitespace extends ArgParser {

  def main(args: Array[String]): Unit = {
    val ops = new WhitespaceOps()
    val options = parseargs(args)
    val loadFile = options.getOrElse("loadFile", throw new RuntimeException("No load file")).toString
    val writeFile = options.getOrElse("writeFile", throw new RuntimeException("No write file")).toString
    val benchFlag = if (options.exists(_._1 == "bench")) {
      options("bench").toString.toBoolean
    } else {
      false
    }
    val printFlag = if (options.exists(_._1 == "print")) {
      options("print").toString.toBoolean
    } else {
      false
    }
    val mmapFlag = if (options.exists(_._1 == "mmap")) {
      true
    } else {
      false
    }
    val driver = new DDLDriver(ops, loadFile, mmapFlag, benchFlag, printFlag) {}
    driver.emitMyCode(writeFile)
  }
}
