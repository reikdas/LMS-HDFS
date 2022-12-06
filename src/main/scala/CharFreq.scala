import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{CCodeGenCMacro, CCodeGenLibFunction, CCodeGenMPI, CCodeGenScannerOps, LibFunction, MPIOps, ScannerOps}
import lms.collection.mutable.ArrayOps
import lms.core.Backend.Node

@virtualize
trait CharFreqOps extends HDFSOps with FileOps with MyMPIOps with CharArrayOps {

  def Mapper(buf: Rep[LongArray[Char]], size: Rep[Long]): Rep[Array[Long]] = {
    val arr = NewArray0[Long](26)
    for (i <- 0 until size.toInt) {
      val c = buf(i).toInt
      if (c >= 65 && c <= 90) {
        arr(c - 65) += 1
      } else if (c >= 97 && c <= 122) {
        arr(c - 97) += 1
      }
    }
    arr
  }

  def Reducer(l: Rep[Array[Long]]): Rep[Long] = {
    var total: Rep[Long] = 0L
    for (i <- 0 until l.length) {
      total = total + l(i)
    }
    total
  }

  def HDFSExec(paths: Rep[Array[String]], benchFlag: Boolean = false, printFlag: Boolean = true) = {
    // MPI initialize
    var world_size = 0
    var rank = 0

    val start = mpi_wtime()
    mpi_init()
    mpi_comm_size(mpi_comm_world, world_size)
    mpi_comm_rank(mpi_comm_world, rank)

    // Allocate mappers and reducers to processes
    if (world_size > paths.length) world_size = paths.length // max_procs = num_blocks
    val blocks_per_proc = paths.length / world_size
    var remaining_map = 0 // Num_blocks that can't be evenly divided among procs
    if (paths.length % world_size != 0) remaining_map = paths.length % world_size
    val chars_per_proc = 26 / world_size
    val limit = chars_per_proc * world_size // Total chars that can be evenly divided among procs


    val foo: Rep[Int] = world_size // Had to cast Var to Rep for mod
    val remaining_red = 26 % foo // Chars that can't be evenly divided among procs

    // Dont need more processes than world_size
    if (rank < world_size) {
      val buf = NewLongArray[Char](GetBlockLen() + 1, Some(0))
      for (i <- 0 until blocks_per_proc) {
        val idx = (rank * blocks_per_proc) + i
        val block_num = open(paths(idx))
        val size = filelen(block_num)
        val readbuf = readFile(block_num, buf, size)
        val charmap = Mapper(readbuf.value, readbuf.length) // readlen can be < buf.length for last block
        for (j <- 0 until limit: Rep[Range]) {
          val dest = j / chars_per_proc
          // (rank * 100) to prevent blocking on receiving same char from different thread
          mpi_send(charmap(j), 1, mpi_long, dest, (rank*100) + j, mpi_comm_world)
        }
        for (j <- 0 until remaining_red: Rep[Range]) {
          mpi_send(charmap(limit + j), 1, mpi_long, j, (rank*100) + j+limit, mpi_comm_world)
        }
        charmap.free
      }
      if (rank < remaining_map) { // Same as above for remaining_map
        val idx = ((paths.length / world_size) * world_size) + rank
        val block_num = open(paths(idx))
        val size = filelen(block_num)
        val readbuf = readFile(block_num, buf, size)
        val charmap = Mapper(readbuf.value, readbuf.length)
        for (j <- 0 until limit: Rep[Range]) {
          val dest = j / chars_per_proc
          mpi_send(charmap(j), 1, mpi_long, dest, (rank*100) + j, mpi_comm_world)
        }
        for (j <- 0 until remaining_red: Rep[Range]) { // Same as above loop for remaining_red
          mpi_send(charmap(limit + j), 1, mpi_long, j, (rank*100) + j + limit, mpi_comm_world)
        }
        charmap.free
      }

      // Reducer
      val reducearg = NewArray[Long](paths.length)
      for (i <- 0 until chars_per_proc: Rep[Range]) {
        val tag = (rank * chars_per_proc) + i
        for (j <- 0 until paths.length - remaining_map) {
          val src = j/blocks_per_proc
          mpi_rec(reducearg(j), 1, mpi_long, src, (src * 100) + tag, mpi_comm_world)
        }
        for (j <- 0 until remaining_map: Rep[Range]) { // Same as above loop for remaining_map
          mpi_rec(reducearg(paths.length - remaining_map + j), 1, mpi_long, j, (j*100) + tag, mpi_comm_world)
        }
        // Send to aggregator node
        mpi_send(Reducer(reducearg), 1, mpi_long, 0, tag, mpi_comm_world)
      }
      if (rank < remaining_red) { // Same as above for remaining_red
        for (i <- 0 until paths.length - remaining_map) {
          val src = i/blocks_per_proc
          mpi_rec(reducearg(i), 1, mpi_long, src, (src * 100) + rank + limit, mpi_comm_world)
        }
        for (i <- 0 until remaining_map: Rep[Range]) { // Same as above loop for remaining_map
          mpi_rec(reducearg(paths.length - remaining_map + i), 1, mpi_long, i, (i*100) + rank + limit, mpi_comm_world)
        }
        // Send to aggregator node
        mpi_send(Reducer(reducearg), 1, mpi_long, 0, rank + limit, mpi_comm_world)
      }
    }

    // Aggregate on node 0
    val res = NewArray0[Int](26)
    if (rank == 0) {
      for (i <- 0 until limit: Rep[Range]) {
        mpi_rec(res(i), 1, mpi_long, i/chars_per_proc, i, mpi_comm_world)
      }
      for (i <- 0 until remaining_red: Rep[Range]) { // Same as above loop for remaining_red
        mpi_rec(res(limit + i), 1, mpi_long, i, limit + i, mpi_comm_world)
      }
      if (printFlag) {
        for (i <- 0 until 26: Rep[Range]) {
          printf("%c = %ld\n", (i + 65), res(i))
        }
      }
    }

    // Finish MPI stuff
    mpi_finalize()
    if (benchFlag) {
      val end = mpi_wtime()
      printf("Proc %d spent %lf time.\n", rank, end - start)
    }

    res
  }
}

object CharFreq {

  def main(args: Array[String]): Unit = {

    implicit class RegexOps(sc: StringContext) {
      def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }

    val options: Map[String, Any] = args.toList.foldLeft(Map[String, Any]()) {
      case (options, r"--loadFile=(\/\w+.txt)$e") => options + ("loadFile" -> e)
      case (options, r"--writeFile=(\w+.c)$e") => options + ("writeFile" -> e)
      case (options, "--bench") => options + ("bench" -> true)
      case (options, "--print") => options + ("print" -> true)
    }

    val loadFile = options.getOrElse("loadFile", throw new RuntimeException("No load file")).toString
    val writeFile = options.getOrElse("writeFile", throw new RuntimeException("No write file")).toString
    val benchFlag: Boolean = if (options.exists(_._1 == "bench")) { options("bench").toString.toBoolean } else { false }
    val printFlag: Boolean = if (options.exists(_._1 == "print")) { options("print").toString.toBoolean } else { false }

    val driver = new DslDriverC[Int, Unit] with CharFreqOps {
      q =>
      override val codegen = new DslGenC with CCodeGenLibFunction with CCodeGenMPI with CCodeGenCMacro with CCodeGenScannerOps {
        registerHeader("<ctype.h>")
        registerHeader("src/main/resources/headers", "\"ht.h\"")
        val IR: q.type = q
      }

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val paths = GetPaths(loadFile)
        val res = HDFSExec(paths, benchFlag, printFlag)
        paths.free
      }

      def emitMyCode(path: String) = {
        codegen.emitSource[Int, Unit](wrapper, "Snippet", new java.io.PrintStream(path))
      }
    }
    driver.emitMyCode(writeFile)
  }
}