import lms.core.stub._
import lms.macros.SourceContext
import lms.core.{virtualize, Backend}
import lms.thirdparty.{CCodeGenLibFunction, LibFunction, ScannerOps}

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import sys.process._

trait FileOps extends ScannerOps with LibFunction {
  def read(fd: Rep[Int], buf: Rep[Array[Char]], size: Rep[Long]): Rep[Int] =
    libFunction[Int]("read", Unwrap(fd), Unwrap(buf), Unwrap(size))(Seq(0, 1, 2), Seq(1), Set())
}

trait HDFSOps {
  def GetPaths(path: String): ListBuffer[String] = {
    val basepath =
      "hdfs getconf -confKey dfs.datanode.data.dir".!!.replaceAll("\n", "")
    val result = "hdfs fsck %s -files -blocks -locations".format(path)
    val output = result.!!
    val lines = output.split("\n")
    var count = 0
    var size = 0
    val blocks_infos = new ListBuffer[String]()
    val dnodes_infos = new ListBuffer[String]()
    val ips_infos = new ListBuffer[ListBuffer[String]]()
    for (line <- lines) {
      val words = line.split(" ")
      var flag = true
      if (line.nonEmpty && count == 0) {
        if (words(0).equals(path)) {
          count = count + 1
          size = words(1).toInt
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
    val output = "/usr/lib/hadoop-3.3.2/bin/hdfs getconf -confKey dfs.blocksize".!!
    output.replace("\n", "").toInt
  }
}

trait MapReduceOps extends FileOps with HDFSOps {

  abstract class MapReduceComputation[MapperResult: Manifest, ReducerResult] {
    def Mapper(buf: Rep[Array[Char]]): Rep[MapperResult]

    def Reducer(someList: Rep[Array[MapperResult]]): Rep[ReducerResult]
  }

  def HDFSExec[MapperResult: Manifest, ReducerResult](
      filename: String,
      mapReduce: MapReduceComputation[MapperResult, ReducerResult]) = {
    val paths = GetPaths(filename)
    val map_result = NewArray[MapperResult](paths.length)
    val buf = NewArray[Char](GetBlockLen() + 1)
    for (i <- 0 until paths.length: Range) {
      val block_num = open(paths(i))
      val size = filelen(block_num)
      read(block_num, buf, size)
      map_result(i) = mapReduce.Mapper(buf)
    }
    mapReduce.Reducer(map_result)
  }
}

trait MyFoo extends MapReduceOps {
  @virtualize
  case class MyComputation() extends MapReduceComputation[Int, Long] {
    override def Mapper(buf: Rep[Array[Char]]): Rep[Int] = {
      var count = 0
      var i = 0
      while (i < buf.length) {
        if (buf(i) != ' ') count = count + 1
        i += 1
      }
      count
    }

    override def Reducer(counts: Rep[Array[Int]]): Rep[Long] = {
      var total = 0L
      var i = 0
      while (i < counts.length) {
        total += counts(i)
        i += 1
      }
      total
    }
  }
}

object Main {

  def main(args: Array[String]): Unit = {
    val snippet = new DslDriverC[Int, Unit] with MyFoo {
      q =>
      override val codegen = new DslGenC with CCodeGenLibFunction {
        val IR: q.type = q
      }

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val count = HDFSExec("/1G.txt", MyComputation())
        println(count)
      }
    }

    println(snippet.code)
  }
}

/*

  def verifyChecksum(blocks: ListBuffer[String], filename: String): Unit = {

    val localhash = {
      val sb = new mutable.StringBuilder("")
      for (i <- blocks.indices) {
        sb ++= blocks(i)
        sb ++= " "
      }
      val localcheckcom = "md5sum %s | md5sum".format(sb)
      println(localcheckcom)
      val localcheck = localcheckcom.!!
      localcheck
    }

    val hdfshash = {

    }
  }


  val filename = "/1G.txt"
  val blocks = GetPaths(filename)
  verifyChecksum(blocks, filename)*/
