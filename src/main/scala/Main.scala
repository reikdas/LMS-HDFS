import lms.collection.immutable.{CppCodeGen_List, CppCodeGen_Tuple, ListOps, TupleOps}
import lms.core.stub._
import lms.macros.SourceContext
import lms.core.{Backend, virtualize}
import lms.thirdparty.{CCodeGenLibFunction, LibFunction, ScannerOps}
import lms.collection.mutable.ArrayOps

import java.io.File
import scala.collection.mutable.ListBuffer
import sys.process._

trait FileOps extends ScannerOps with LibFunction {
  def read(fd: Rep[Int], buf: Rep[Array[Char]], size: Rep[Long]): Rep[Int] =
    libFunction[Int]("read", Unwrap(fd), Unwrap(buf), Unwrap(size))(Seq(0, 1, 2), Seq(1), Set())
}

trait LMSMore extends LibFunction with ArrayOps {
  def strcmp(str1: Rep[String], str2: Rep[String]): Rep[Int] =
    libFunction[Int]("strcmp", Unwrap(str1), Unwrap(str2))(Seq(0, 1), Seq(), Set())

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
trait MapReduceOps extends FileOps with HDFSOps with ListOps with TupleOps with LMSMore {

  abstract class MapReduceComputation[KeyType: Manifest, ValueType: Manifest, ReducerResult: Manifest] {
    def Mapper(buf: Rep[Array[Char]]): RepArray[Tuple2[KeyType, ValueType]]

    def Reducer(l: Rep[Array[Tuple2[KeyType, ValueType]]]): Rep[ReducerResult]
  }

  def sort[KeyType: Manifest : Ordering, ValueType: Manifest]
  (arr: RepArray[Tuple2[KeyType, ValueType]]): RepArray[Tuple2[KeyType, ValueType]] = {
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

  def mergeSort[KeyType: Manifest: Ordering, ValueType: Manifest]
  (arr: Rep[Array[Tuple2[KeyType, ValueType]]], indexes: Rep[Array[Int]]): Rep[Array[Tuple2[KeyType, ValueType]]] = {
    arr
    //???
  }

  def HDFSExec[KeyType: Manifest: Ordering, ValueType: Manifest, ReducerResult: Manifest](
      filename: String,
      mapReduce: MapReduceComputation[KeyType, ValueType, ReducerResult]) = {
    val paths = GetPaths(filename)
    val buf = NewArray[Char](GetBlockLen() + 1)
    // Run first mapper independently to calculate length of final arr
    val block_num_f = open(paths.head)
    val size_f = filelen(block_num_f)
    val out_f = read(block_num_f, buf, size_f)
    println(out_f) // FIXME: Print only to force read not to be DCE'd
    val arr_f = sort(mapReduce.Mapper(buf))
    val arr = NewArray[Tuple2[KeyType, ValueType]](arr_f.length * paths.length * 1.5) // Array storing all words
    val ends = NewArray[Int](paths.length) // Array storing last indices of each mapper result in arr
    for (i <- 0 until arr_f.length) {
      arr(i) = arr_f(i)
    }
    ends(0) = arr_f.length
    // Run mapper on all the other blocks
    for (i <- 1 until paths.length: Range) {
      val block_num = open(paths(i))
      val size = filelen(block_num)
      val out = read(block_num, buf, size)
      println(out) // FIXME: Print only to force read not to be DCE'd
      val wordlist = sort(mapReduce.Mapper(buf))
      for (j <- 0 until wordlist.length) {
        arr(ends(i-1) + j) = wordlist(j)
      }
      ends(i) = wordlist.length
    }
    val msort_arr = mergeSort(arr, ends)
    var idx_start = 0
    val logical_end = ends(paths.length - 1)
    while (idx_start < logical_end) {
      var idx_end = idx_start + 1
      val key = msort_arr(idx_start)._1
      while (msort_arr(idx_end)._1 == key && idx_end <= logical_end) idx_end = idx_end + 1
      println(key)
      println(mapReduce.Reducer(msort_arr.slice(idx_start, idx_end)))
      idx_start = idx_end
    }
  }
}

trait MyFoo extends MapReduceOps with ListOps with TupleOps with ArrayOps {
  @virtualize
  case class MyComputation() extends MapReduceComputation[String, Int, Int] {

    override def Mapper(buf: Rep[Array[Char]]): RepArray[Tuple2[String, Int]] = {
      var start = 0
      var count = 0
      while (start < (buf.length - 1)) {
        while (buf(start) == ' ' || buf(start) == '\n' && start < (buf.length - 1)) start = start + 1
        var end = start + 1
        while ((buf(end) != ' ' || buf(end) != '\n') && (end < buf.length)) end = end + 1
        count = count + 1
        start = end
      }
      val wordlist = NewArray[Tuple2[String, Int]](count)
      start = 0
      var idx = 0
      while (start < (buf.length - 1)) {
        while (buf(start) == ' ' || buf(start) == '\n' && start < (buf.length - 1)) start = start + 1
        var end = start + 1
        while ((buf(end) != ' ' || buf(end) != '\n') && (end < buf.length)) end = end + 1
        wordlist(idx) = Tuple2[String, Int](buf.slice(start, end).ArrayOfCharToString(), 1)
        start = end
        idx = idx + 1
      }
      RepArray(wordlist, count)
    }

    override def Reducer(l: Rep[Array[Tuple2[String, Int]]]): Rep[Int] = {
      var total = 0
      for (i <- 0 until l.length) {
        total = total + l(i)._2
      }
      total
    }
  }
}

object Main {

  def main(args: Array[String]): Unit = {
    val snippet = new DslDriverCPP[Int, Unit] with MyFoo {
      q =>
      override val codegen = new DslGenCPP with CppCodeGen_List with CppCodeGen_Tuple with CCodeGenLibFunction {
        val IR: q.type = q
      }

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val res = HDFSExec("/mr_input/wordcount/wc01.txt", MyComputation())
        /*for (i <- 0 until res.length: Rep[Range]) {
          println(res(i)._1)
          println(res(i)._2)
        }*/
        println(res)
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
