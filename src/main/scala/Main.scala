import flare.{Config, FlareBackend, FlareCCodegen, FlareOps}
import lms.collection.immutable.{ListOps, TupleOps}
import lms.core.stub._
import lms.macros.SourceContext
import lms.core.{Backend, virtualize}
import lms.thirdparty.{CCodeGenLibFunction, LibFunction, ScannerOps}
import flare.common.RecordOps
import lms.collection.mutable.ArrayOps
import flare.collection.BufferOps
import flare.debug.LOGLevel

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
trait MapReduceOps extends FileOps with HDFSOps with ListOps with TupleOps {

  abstract class MapReduceComputation[KeyType: Manifest, ValueType: Manifest, ReducerResult] {
    def Mapper(buf: Rep[Array[Char]]): Rep[Array[Tuple2[KeyType, ValueType]]]

    def Reducer(someArr: Rep[Array[Tuple2[KeyType, ValueType]]]): Rep[ReducerResult]
  }

  def sort[KeyType: Manifest: Ordering, ValueType:Manifest]
  (arr: Rep[Array[Tuple2[KeyType, ValueType]]]): Rep[Array[Tuple2[KeyType, ValueType]]] = {
    for (i <- 0 until (arr.length - 1)) {
      var min_idx = i
      for (j <- (i+1) until arr.length) {
        if (arr(j)._1 < arr(min_idx)._1) min_idx = j
      }
      val temp = arr(min_idx)
      arr(min_idx) = arr(i)
      arr(i) = temp
    }
    arr
  }

  def mergeSort[KeyType: Manifest: Ordering, ValueType: Manifest]
  (arrs: Rep[List[Array[Tuple2[KeyType, ValueType]]]]): Rep[Array[Tuple2[KeyType, ValueType]]] = {
    var mergedList: Rep[List[Tuple2[KeyType, ValueType]]] = List()
    for (i <- 0 until arrs.size) {
      for (j <- 0 until arrs(i).length) {
        mergedList = arrs(i)(j)::mergedList
      }
    }
    sort(mergedList.toArray)
  }

  def HDFSExec[KeyType: Manifest: Ordering, ValueType: Manifest, ReducerResult](
      filename: String,
      mapReduce: MapReduceComputation[KeyType, ValueType, ReducerResult]) = {
    val paths = GetPaths(filename)
    val buf = NewArray[Char](GetBlockLen() + 1)
    var map_result: Rep[List[Array[Tuple2[KeyType, ValueType]]]] = List()
    for (i <- 0 until paths.length: Range) {
      val block_num = open(paths(i))
      val size = filelen(block_num)
      read(block_num, buf, size)
      map_result = mapReduce.Mapper(buf) :: map_result
    }
    val flattenedarr: Rep[Array[Tuple2[KeyType, ValueType]]] = mergeSort(map_result)
    var start = 0
    while (start < (flattenedarr.length - 1)) {
      var end = start + 1
      val key = flattenedarr(start)._1
      while (flattenedarr(end)._1 == key && end < flattenedarr.length) end = end + 1
      println(mapReduce.Reducer(flattenedarr.slice(start, end)))
      start = end
    }
  }
}

trait MyFoo extends MapReduceOps with ListOps with TupleOps with ArrayOps {
  @virtualize
  case class MyComputation() extends MapReduceComputation[String, Int, Tuple2[String, Int]] {

    override def Mapper(buf: Rep[Array[Char]]): Rep[Array[Tuple2[String, Int]]] = {
      var wordlist: Rep[List[Tuple2[String, Int]]] = List()
      var start = 0
      while (start < (buf.length - 1)) {
        while (buf(start) == ' ' || buf(start) == '\n' && start < (buf.length - 1)) start = start + 1
        var end = start + 1
        while ((buf(end) != ' ' || buf(end) != '\n') && (end < buf.length)) end = end + 1
        wordlist = Tuple2[String, Int](buf.slice(start, end).toString, 1)::wordlist
        start = end
      }
      wordlist.toArray
    }

    override def Reducer(arr: Rep[Array[Tuple2[String, Int]]]): Rep[Tuple2[String, Int]] = {
      var total = 0
      for (i <- 0 until arr.length) {
        total = total + arr(i)._2
      }
      Tuple2[String, Int](arr(0)._1, total)
    }
  }
}

object myConfig {
  val config = Config(
    folder = "/tmp/",
    boundcheck = true,
    comLogLvl = LOGLevel.ERROR,
    runLogLvl = LOGLevel.ALL)
}

class MyBackend extends FlareBackend(myConfig.config) with FlareOps {
  @virtualize
  def cmain3(argc: Rep[Int], args: Rep[Array[String]]) = {
    val text: Rep[String] = args(0) // "Hi my name is Pratyush "
    val schema = Seq(StringField("Foo", false), IntField("Bar", false))
    val arr = Buffer.flat(schema, 1, 5L)
    var start = 0
    var count = 0L
    while (start < (text.length) - 1) {
      while (text(start) == ' ' && start < (text.length - 1)) start = start + 1
      var end = start
      while (text(end) != ' ' && end < text.length) end = end + 1
      println(start)
      println(end)
      arr.doUpdate(count, Record.column(schema, Seq(StringValue(text.substring(start, end), text.substring(start, end).length, false), IntValue(argc, false))))
      count = count + 1L
      start = end
    }
    var i = 0L
    while (i < count) {
      val stringfield = arr(i)(schema(0).name)
      val intfield = arr(i)(schema(1).name)
      println(stringfield)
      println(intfield)
      i = i + 1
    }
  }


  def readInt: Rep[Int] = unchecked[Int]("read_int(0)")
  def auxReadString(len: Rep[Int]): Rep[String] = unchecked[String]("unsafe_read_str(",len,", 0)")
  def readString = {
    val len = readInt
    (auxReadString(len), len)
  }

  def dummyFuncInt: Rep[Int] = unchecked[Int]("read_int(0)")
  def dummyFuncString: Rep[String] = unchecked[String]("read_string(0)")

  def readRecord(schema: Schema): Record = {
    Record.column(schema, schema map {
      case _: IntField => IntValue(readInt, false)
      case _: StringField =>
        val (str, len) = readString
        StringValue(str, len, false)
    })
  }
  def writeInt(res: Rep[Int]): Rep[Unit] = unchecked[Unit]("write_int(",res,")")
  @virtualize
  def cmain(argc: Rep[Int], args: Rep[Array[String]]) = {
    // We want - arr[(2, "5"), (4, "7"), (5, "9")]
    val input = Seq(2, "5", 4, "7", 5, "9")
    val len = 3 // len(arr)
    val reclen = 2
    val schema = Seq(IntField("Bar", nullable=false), StringField("Foo", nullable = false))
    val nativeBuf = Buffer.flat(schema, 1, 3L)
    /*val text = args(0)
    var cur = 0
    var count: Long = 0
    while (cur < text.length) {
      var end = cur
      println(text.substring(argc + 5, argc + 10))
      //if (text(cur) == ' ')
        nativeBuf(count) = Record.column(schema, Seq(IntValue(1, false), StringValue(text.substring(argc + 5, argc + 10), text.length, false)))
      cur = cur + 1
      count = count + 1
    }*/

    nativeBuf.fill { _ => readRecord(schema) }
    println(nativeBuf(0L)(schema(1).name))


    var st: Int = 0
    for (i <- 0 until len: Range) {
      val rec = nativeBuf(i.toLong)
      val values = input.slice(st, st + reclen)
      st += reclen
      val dummyschema = (rec(schema(0).name) equalsTo Value(values(0))).toBool
      println(dummyschema)
      for ((x, n) <- values zip schema) {
        if ((rec(n.name) equalsTo Value(x)).toBool) println(1) else println(0)
      }
    }
  }

  /*@virtualize
  override def cmain(
                      argc: Rep[Int],
                      args: Rep[Array[String]]
                    ): Rep[Unit] = {
    val schema: Schema = Seq(StringField("Foo", nullable = false))
    val nativeBuf = Buffer.flat(schema, 1, 10L)
    nativeBuf.fill(_ => ColumnRec(schema, Seq(StringValue("thestring", 9, true))))
    println(nativeBuf(argc))
  }*/
}

object Main {

  def main2(args: Array[String]): Unit = {
    val snippet = new DslDriverC[Int, Unit] with MyFoo with BufferOps with RecordOps {
      q =>
      override val codegen = new DslGenC with CCodeGenLibFunction {
        val IR: q.type = q
      }

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        /*val count = HDFSExec("/1G.txt", MyComputation())
        println(count)*/

        ()
      }
    }

    println(snippet.code)
  }

  def main(args: Array[String]): Unit = {
    val backend = new MyBackend()
    backend.stage
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
