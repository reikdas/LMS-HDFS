import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{CCodeGenLibFunction, LibFunction, ScannerOps}
import lms.collection.mutable.ArrayOps

import flare.{Config, FlareBackend, FlareOps}
import flare.debug.LOGLevel
import flare.collection.BufferOps

import java.io.File
import scala.collection.mutable.ListBuffer
import sys.process._

trait FileOps extends LibFunction {
  def read(fd: Rep[Int], buf: Rep[Array[Char]], size: Rep[Long]): Rep[Int] =
    libFunction[Int]("read", Unwrap(fd), Unwrap(buf), Unwrap(size))(Seq(0, 1, 2), Seq(1), Set())
}

trait LMSMore extends BufferOps {

  case class RepArray[T: Manifest](value: Rep[Array[T]], length: Rep[Int]) {
    def apply(idx: Rep[Int]): Rep[T] = value(idx)

    def update(idx: Rep[Int], something: Rep[T]): Unit = {
      value(idx) = something
    }

    def free = value.free
  }

  case class StructArr(arr: FlatBuffer[Struct], count: Long)

  def ListToArr(l: ListBuffer[String]): Rep[Array[String]] = {
    val arr = NewArray[String](l.size)
    for (i <- 0 until l.size: Range) {
      arr(i) = l(i)
    }
    arr
  }

  def sort[KeyType: Manifest : Ordering, ValueType: Manifest]
  (arr: StructArr): StructArr = {
    for (i <- 0 until (arr.length - 1)) {
      var min_idx = i
      for (j <- (i + 1) until arr.length) {
        if (arr(j)._1 < arr(min_idx)._1) min_idx = j
      }
      val temp = arr(min_idx)
      arr(min_idx) = arr(i)
      arr(i) = temp
    }
    arr
  }

  def mergeSort[KeyType: Manifest : Ordering, ValueType: Manifest]
  (arr: Rep[Array[Tuple2[KeyType, ValueType]]], indexes: Rep[Array[Int]]): Rep[Array[Tuple2[KeyType, ValueType]]] = {
    arr
    //???
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

  abstract class MapReduceComputation[KeyType: Manifest, ValueType: Manifest, ReducerResult: Manifest] {

    def Mapper(buf: Rep[Array[Char]]): StructArr

    def Reducer(l: Rep[Array[ValueType]]): Rep[ReducerResult]
  }

  def HDFSExec[KeyType: Manifest, ValueType: Manifest, ReducerResult: Manifest](
      filename: String,
      mapReduce: MapReduceComputation[KeyType, ValueType, ReducerResult]): Rep[Array[ReducerResult]] = {
    val paths = ListToArr(GetPaths(filename))
    val buf = NewArray[Char](GetBlockLen() + 1)
    // Run first mapper independently to calculate length of final arr
    val block_num_f = open(paths(0))
    val size_f = filelen(block_num_f)
    val out_f = read(block_num_f, buf, size_f)
    val arr_f = sort(mapReduce.Mapper(buf))
    val schema = ???
    val arr = Buffer.flat(schema, 1, buf.length)


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
      charmap.free
    }
    val reducearg = NewArray[ValueType](paths.length)
    for (i <- 0 until 26: Range) {
      for (j <- 0 until paths.length: Range) {
        reducearg(j) = mapperout((j*26) + i)
      }
      res(i) = mapReduce.Reducer(reducearg)
    }
    buf.free
    mapperout.free
    reducearg.free
    res
  }
}

trait MyFoo extends MapReduceOps with ArrayOps with FlareOps {
  @virtualize
  case class MyComputation() extends MapReduceComputation[String, Int, Int] {

    override def Mapper(buf: RepArray[Char]): StructArr = {
      val schema: Schema = Seq(StringField("Key", nullable = false), IntField("Value", nullable = false))
      val arr = Buffer.flat(schema, 1, buf.length)
      var start = 0
      var count = 0L
      while (start < (buf.length - 1)) {
        while (buf(start) == ' ' || buf(start) == '\n' && start < (buf.length - 1)) start = start + 1
        var end = start + 1
        while ((buf(end) != ' ' || buf(end) != '\n') && (end < buf.length)) end = end + 1
        val word = buf.value.slice(start, end).ArrayOfCharToString()
        arr(count) = Record.column(schema, Seq(StringValue(word, word.length, false), IntValue(1, false)))
        count = count + 1
      }
      StructArr(arr, count)
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

object myConfig {
  val config = Config(
    folder = "/home/reikdas/lmshdfs-dump/",
    boundcheck = true,
    comLogLvl = LOGLevel.ERROR,
    runLogLvl = LOGLevel.ALL)
}

class MyBackend extends FlareBackend(myConfig.config) with MyFoo {

  @virtualize
  override def cmain(
                      argc: Rep[Int],
                      args: Rep[Array[String]]
                    ): Rep[Unit] = {
    val res = HDFSExec("/1G.txt", MyComputation())
    res.free
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val backend = new MyBackend()
    backend.stage
  }
}
