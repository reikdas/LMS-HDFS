import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{CCodeGenCMacro, CCodeGenLibFunction, CCodeGenMPI, LibFunction, MPIOps, ScannerOps}
import lms.collection.mutable.ArrayOps

import java.io.File
import scala.collection.mutable.ListBuffer
import sys.process._

trait LMSMore extends ArrayOps {

  case class RepArray[T: Manifest](value: Rep[Array[T]], length: Rep[Int]) {
    def apply(idx: Rep[Int]): Rep[T] = value(idx)

    def update(idx: Rep[Int], something: Rep[T]): Unit = {
      value(idx) = something
    }

    def free = value.free
  }

  def ListToArr(l: ListBuffer[String]): Rep[Array[String]] = {
    val arr = NewArray[String](l.size)
    for (i <- 0 until l.size: Range) {
      arr(i) = l(i)
    }
    arr
  }

}

trait FileOps extends LibFunction with ScannerOps with LMSMore {
  def readFile(fd: Rep[Int], buf: Rep[Array[Char]], size: Rep[Long]): RepArray[Char] = {
    val readlen = libFunction[Int]("read", Unwrap(fd), Unwrap(buf), Unwrap(size))(Seq[Int](), Seq(1), Set())
    RepArray[Char](buf, readlen)
  }

  def mmapFile(fd: Rep[Int], buf: Rep[Array[Char]], size: Rep[Long]): RepArray[Char] = {
    val newbuf = mmap[Char](fd, size)
    RepArray[Char](newbuf, size.toInt)
  }
}

trait HDFSOps {
  def GetPaths(path: String): ListBuffer[String] = {
    val basepath =
      "hdfs getconf -confKey dfs.datanode.data.dir".!!.replaceAll("\n", "")
    val result = "hdfs fsck %s -files -blocks -locations".format(path)
    val output = result.!!
    val lines = output.split("\n")
    var count = 0
    var size: Long = 0
    val blocks_infos = new ListBuffer[String]()
    val dnodes_infos = new ListBuffer[String]()
    val ips_infos = new ListBuffer[ListBuffer[String]]()
    for (line <- lines) {
      val words = line.split(" ")
      var flag = true
      if (line.nonEmpty && count == 0) {
        if (words(0).equals(path)) {
          count = count + 1
          size = words(1).toLong
          flag = false
        } else {
          flag = true
        }
      }
      if (flag && line.nonEmpty && count > 0) {
        if (words(0).nonEmpty) {
          if (words(0)
                .substring(0, words(0).length - 1)
                .forall(Character.isDigit)) {
            if (words(0).substring(0, words(0).length - 1).toInt == count - 1) {
              count = count + 1
              val rawinfo = words(1).split(":")
              blocks_infos += rawinfo(1).substring(0, rawinfo(1).lastIndexOf("_"))
              dnodes_infos += rawinfo(0)
              val stline =
                line.substring(line.indexOfSlice("DatanodeInfoWithStorage") - 1, line.size)
              val rawparts = stline.split(" ")
              val ips = new ListBuffer[String]()
              for (part <- rawparts) {
                val index = part.indexOfSlice("DatanodeInfoWithStorage")
                ips += part.substring(index + "DatanodeInfoWithStorage".length(), part.indexOf(","))
              }
              ips_infos += ips
            }
          }
        }
      }
    }
    assert(size > 0)
    assert(blocks_infos.size == dnodes_infos.size)
    val nativepaths = new ListBuffer[String]

    def getListOfFiles(dir: File): Array[File] = {
      val these = dir.listFiles
      these ++ these.filter(_.isDirectory).flatMap(getListOfFiles)
    }

    for (i <- blocks_infos.indices) {
      val allpaths = getListOfFiles(
        new File(basepath + "/current/%s/current/finalized".format(dnodes_infos(i))))
      var flag = 0
      for (j <- allpaths.indices) {
        if (allpaths(j).toString.contains(blocks_infos(i)) && !allpaths(j).toString
              .contains(".meta") && flag == 0) {
          nativepaths += allpaths(j).toString
          flag = 1
        }
      }
      if (flag == 0)
        throw new Exception("Should be unreachable")
    }
    nativepaths
  }

  def GetBlockLen(): Int = {
    val output = "hdfs getconf -confKey dfs.blocksize".!!
    output.replace("\n", "").toInt
  }
}

@virtualize
trait MapReduceOps extends FileOps with ScannerOps with HDFSOps with MPIOps {

  abstract class MapReduceComputation[ValueType: Manifest, ReducerResult: Manifest] {
    def Mapper(buf: Rep[Array[Char]], size: Rep[Long]): Rep[Array[ValueType]]

    def Reducer(l: Rep[Array[ValueType]]): Rep[ReducerResult]
  }

  def HDFSExec[ValueType: Manifest, ReducerResult: Manifest](
      filename: String,
      mapReduce: MapReduceComputation[ValueType, ReducerResult]): Rep[Array[ReducerResult]] = {
    val paths = ListToArr(GetPaths(filename))

    // MPI initialize
    var world_size = 0
    var rank = 0
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
      val buf = NewArray[Char](GetBlockLen() + 1)
      for (i <- 0 until blocks_per_proc) {
        val idx = (rank * blocks_per_proc) + i
        val block_num = open(paths(idx))
        val size = filelen(block_num)
        val readbuf = readFile(block_num, buf, size)
        val charmap = mapReduce.Mapper(readbuf.value, readbuf.length) // readlen can be < buf.length for last block
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
        val charmap = mapReduce.Mapper(readbuf.value, readbuf.length)
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
      val reducearg = NewArray[ValueType](paths.length)
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
        mpi_send(mapReduce.Reducer(reducearg), 1, mpi_long, 0, tag, mpi_comm_world)
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
        mpi_send(mapReduce.Reducer(reducearg), 1, mpi_long, 0, rank + limit, mpi_comm_world)
      }
    }

    // Aggregate on node 0
    val res = NewArray0[ReducerResult](26)
    if (rank == 0) {
      for (i <- 0 until limit: Rep[Range]) {
        mpi_rec(res(i), 1, mpi_long, i/chars_per_proc, i, mpi_comm_world)
      }
      for (i <- 0 until remaining_red: Rep[Range]) { // Same as above loop for remaining_red
        mpi_rec(res(limit + i), 1, mpi_long, i, limit + i, mpi_comm_world)
      }
      // FIXME: Move this print loop to main
      // This print is also parallelized across procs
      for (i <- 0 until 26: Rep[Range]) {
        printf("%c = %d\n", (i + 65), res(i))
      }
    }

    // Finish MPI stuff
    mpi_finalize()

    paths.free
    res
  }
}

trait MyFoo extends MapReduceOps with ArrayOps {
  @virtualize
  case class MyComputation() extends MapReduceComputation[Long, Long] {

    override def Mapper(buf: Rep[Array[Char]], size: Rep[Long]): Rep[Array[Long]] = {
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

    override def Reducer(l: Rep[Array[Long]]): Rep[Long] = {
      var total = 0L
      for (i <- 0 until l.length) {
        total = total + l(i)
      }
      total
    }
  }
}

object Main {

  def main(args: Array[String]): Unit = {
    val snippet = new DslDriverC[Int, Unit] with MyFoo {
      q =>
      override val codegen = new DslGenC with CCodeGenLibFunction with CCodeGenMPI with CCodeGenCMacro {
        registerHeader("\"scanner_header.h\"")
        val IR: q.type = q
      }

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val res = HDFSExec("/10G.txt", MyComputation())

        res.free
      }
    }
    println(snippet.code)
  }
}
