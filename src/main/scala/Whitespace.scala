import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize

@virtualize
class WhitespaceOps extends DDLoader {

  def HDFSExec(paths: Rep[Array[String]], readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char], benchFlag: Boolean = false, printFlag: Boolean = true, nproc: Boolean = true) = {
    // MPI initialize
    var world_size = 0
    var world_rank = 0
    
    
    if (nproc) {
      mpi_init()
      mpi_comm_size(mpi_comm_world, world_size)
      mpi_comm_rank(mpi_comm_world, world_rank)
      var send: Var[Long] = var_new(1L)
      var rec: Var[Long] = var_new(0L)
      mpi_reduce3(send, rec, 1, mpi_long, mpi_sum, 0, mpi_comm_world)
    }

    val start = timestamp
    Adapter.g.reflectWrite("printflag", Unwrap(start))(Adapter.CTRL)
    val buf = NewLongArray[Char](getBlockLen() + 1, Some(0)) // Buffer to hold characters in file

    if (nproc) {
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
          munmap(fpointer.value, size)
          close(block_num)
        }
      }
      val tmp: Rep[Long] = readVar(count)
      var msg_count: Var[Long] = var_new(tmp)
      Adapter.g.reflectWrite("printflag", Unwrap(msg_count))(Adapter.CTRL)
      val total_count: Var[Long] = var_new(0L)
      mpi_reduce(msg_count, total_count, 1, mpi_long, mpi_sum, 0, mpi_comm_world)
      if (benchFlag) {
        val end = timestamp
        Adapter.g.reflectWrite("printflag", Unwrap(end))(Adapter.CTRL)
        printf("Proc %d spent %ld time.\n", world_rank, end - start)
      }
      if (printFlag) {
        if (world_rank == 0) {
          printf("%ld\n", total_count)
        }
      }
      mpi_finalize()
    } else {
      var count = 0L
      for (i <- 0 until paths.length) {

        // Get buffer of chars from file
        val block_num = open(paths(i))
        val size = filelen(block_num)
        val fpointer = mmapFile(block_num, buf, size)

        for (j <- 0 until fpointer.length) {
          if (fpointer(j) == ' ') {
            count = count + 1L
          }
        }
        munmap(fpointer.value, size)
        close(block_num)
      }
      if (benchFlag) {
        val end = timestamp
        Adapter.g.reflectWrite("printflag", Unwrap(end))(Adapter.CTRL)
        printf("Proc %d spent %ld time.\n", world_rank, end - start)
      }
      if (printFlag) {
          printf("%ld\n", count)
      } else {
        Adapter.g.reflectWrite("printflag", Unwrap(count))(Adapter.CTRL)
      }
    }
    buf.free
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
    val nprocflag = if (options.exists(_._1 == "multiproc")) {
      true
    } else {
      false
    }
    val driver = new DDLDriver(ops, loadFile, mmapFlag, benchFlag, printFlag, nprocflag) {}
    driver.emitMyCode(writeFile)
  }
}
