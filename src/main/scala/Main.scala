import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{CCodeGenLibFunction, LibFunction, ScannerOps}
import lms.collection.mutable.ArrayOps

import java.io.File
import scala.collection.mutable.ListBuffer
import sys.process._

trait FileOps extends LibFunction {
  def read(fd: Rep[Int], buf: Rep[Array[Char]], size: Rep[Long]): Rep[Int] =
    libFunction[Int]("read", Unwrap(fd), Unwrap(buf), Unwrap(size))(Seq(0, 1, 2), Seq(1), Set())
}

trait LMSMore extends ArrayOps {

  case class RepArray[T: Manifest](value: Rep[Array[T]], length: Rep[Int]) {
    def apply(idx: Rep[Int]): Rep[T] = value(idx)

    def update(idx: Rep[Int], something: Rep[T]): Unit = {
      value(idx) = something
    }
  }
}

trait HDFSOps {
  def GetPaths(path: String): ListBuffer[String] = {
    val basepath =
      "/usr/lib/hadoop-3.3.2/bin/hdfs getconf -confKey dfs.datanode.data.dir".!!.replaceAll("\n", "")
    val result = "/usr/lib/hadoop-3.3.2/bin/hdfs fsck %s -files -blocks -locations".format(path)
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

@virtualize
trait MapReduceOps extends FileOps with ScannerOps with HDFSOps with LMSMore {

  abstract class MapReduceComputation[ValueType: Manifest, ReducerResult: Manifest] {
    def Mapper(buf: RepArray[Char]): RepArray[ValueType]

    def Reducer(l: Rep[Array[ValueType]]): Rep[ReducerResult]
  }

  def HDFSExec[ValueType: Manifest, ReducerResult: Manifest](
      filename: String,
      mapReduce: MapReduceComputation[ValueType, ReducerResult]): Rep[Array[ReducerResult]] = {
    val paths = GetPaths(filename)
    val buf = NewArray[Char](GetBlockLen() + 1)
    val mapperout = NewArray[ValueType](26 * paths.length)
    val res = NewArray0[ReducerResult](26)
    for (i <- 0 until paths.length: Range) {
      val block_num = open(paths(i))
      val size = filelen(block_num)
      val out = read(block_num, buf, size)
      val charmap = mapReduce.Mapper(RepArray(buf, out))
      for (j <- 0 until 26: Range) {
        mapperout((i * 26) + j) = charmap(j)
      }
    }
    val reducearg = NewArray[ValueType](paths.length)
    for (i <- 0 until 26: Range) {
      for (j <- 0 until paths.length: Range) {
        reducearg(j) = mapperout((j*26) + i)
      }
      res(i) = mapReduce.Reducer(reducearg)
    }
    res
  }
}

trait MyFoo extends MapReduceOps with ArrayOps {
  @virtualize
  case class MyComputation() extends MapReduceComputation[Int, Int] {

    override def Mapper(buf: RepArray[Char]): RepArray[Int] = {
      val arr = NewArray0[Int](26)
      for (i <- 0 until buf.length) {
        val c = buf(i).toInt
        if (c >= 65 && c <= 90) {
          arr(c - 65) += 1
        } else if (c >= 97 && c <= 122) {
          arr(c - 97) += 1
        }
      }
      RepArray(arr, 26)
    }

    override def Reducer(l: Rep[Array[Int]]): Rep[Int] = {
      var total = 0
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
      override val codegen = new DslGenC with CCodeGenLibFunction {
        registerHeader("/home/reikdas/Research/lms-clean/src/main/resources/headers/", "\"scanner_header.h\"")
        val IR: q.type = q
      }


      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val res = HDFSExec("/mr_input/wordcount/wc01.txt", MyComputation())
        for (i <- 0 until 26: Range) {
          printf("%c = %d\n", (i+65), res(i))
        }
      }
    }
    println(snippet.code)
  }
}
