import lms.macros.SourceContext
import lms.core.virtualize

@virtualize
class CharFreqOps extends DDLoader {

  def HDFSExec(paths: Rep[Array[String]], readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char], benchFlag: Boolean = false, printFlag: Boolean = true) = {
    // MPI initialize
    var world_size = 0
    var rank = 0

    val start = mpi_wtime()
    mpi_init()
    mpi_comm_size(mpi_comm_world, world_size)
    mpi_comm_rank(mpi_comm_world, rank)

    if (rank < paths.length) {
      val buf = NewLongArray[Char](GetBlockLen() + 1, Some(0)) // Buffer to hold characters in file
      val arr = NewArray0[Long](26)
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

      if (printFlag) {
        if (rank == 0) {
          for (i <- 0 until 26: Rep[Range]) {
            printf("%c %ld\n", (i + 65), total_count(i))
          }
        }
      }
      total_count.free
      arr.free
      buf.free
    }
    // Finish MPI stuff
    mpi_finalize()
    if (benchFlag) {
      val end = mpi_wtime()
      printf("Proc %d spent %lf time.\n", rank, end - start)
    }
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
    val driver = new DDLDriver(ops, loadFile, mmapFlag, benchFlag, printFlag) {}
    driver.emitMyCode(writeFile)
  }
}
