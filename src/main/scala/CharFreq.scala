import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize

@virtualize
class CharFreqOps extends DDLoader {

  def HDFSExec(paths: Rep[Array[String]], readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char], benchFlag: Boolean = false, printFlag: Boolean = true, nproc: Boolean) = {
    // MPI initialize
    var world_size = 0
    var rank = 0

    val start = timestamp
    Adapter.g.reflectWrite("printflag", Unwrap(start))(Adapter.CTRL)
    if (nproc) {
      mpi_init()
      mpi_comm_size(mpi_comm_world, world_size)
      mpi_comm_rank(mpi_comm_world, rank)
    }

    val buf = NewLongArray[Char](getBlockLen() + 1, Some(0)) // Buffer to hold characters in file
    val arr = NewArray0[Long](26)

    if (nproc) {
      for (i <- 0 until paths.length) {
        if (i % world_size == rank) {
          // Get buffer of chars from file
          val block_num = open(paths(i))
          val size = filelen(block_num)
          val fpointer = mmapFile(block_num, buf, size)

          for (j <- 0 until fpointer.length) {
            val c = fpointer(j).toInt
            if (c >= 65 && c <= 90) {
              arr(c - 65) += 1
            } else if (c >= 97 && c <= 122) {
              arr(c - 97) += 1
            }
          }
          close(block_num)
        }
      }
      val total_count = NewArray0[Long](26)
      mpi_reduce2(arr, total_count, 26, mpi_long, mpi_sum, 0, mpi_comm_world)
      if (benchFlag) {
        val end = timestamp
        Adapter.g.reflectWrite("printflag", Unwrap(end))(Adapter.CTRL)
        printf("Proc %d spent %ld time.\n", rank, end - start)
      }
      mpi_finalize()
      if (printFlag) {
        if (rank == 0) {
          for (i <- 0 until 26: Rep[Range]) {
            printf("%c %ld\n", (i + 65), total_count(i))
          }
        }
      }
      total_count.free
    } else {
      for (i <- 0 until paths.length) {
        // Get buffer of chars from file
        val block_num = open(paths(i))
        val size = filelen(block_num)
        val fpointer = mmapFile(block_num, buf, size)

        for (j <- 0 until fpointer.length) {
          val c = fpointer(j).toInt
          if (c >= 65 && c <= 90) {
            arr(c - 65) += 1
          } else if (c >= 97 && c <= 122) {
            arr(c - 97) += 1
          }
        }
        close(block_num)
      }
      if (benchFlag) {
        val end = timestamp
        Adapter.g.reflectWrite("printflag", Unwrap(end))(Adapter.CTRL)
        printf("Proc %d spent %ld time.\n", rank, end - start)
      }
      if (printFlag) {
          for (i <- 0 until 26: Rep[Range]) {
            printf("%c %ld\n", (i + 65), arr(i))
          }
      } else {
        Adapter.g.reflectWrite("printflag", Unwrap(arr))(Adapter.CTRL)
      }
    }
    buf.free
    arr.free
    paths.free
  }
}

object CharFreq extends ArgParser {

  def main(args: Array[String]): Unit = {
    val ops = new CharFreqOps()
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
