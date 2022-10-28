import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{CCodeGenLibFunction, LibFunction, ScannerOps}
import lms.collection.mutable.ArrayOps

import java.io.File
import scala.collection.mutable.ListBuffer
import sys.process._

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

@virtualize
trait LMSMore extends ArrayOps with RangeOps {

  case class RepArray[T: Manifest](value: Rep[Array[T]], length: Rep[Int]) {
    def apply(idx: Rep[Int]): Rep[T] = value(idx)

    def update(idx: Rep[Int], something: Rep[T]): Unit = {
      value(idx) = something
    }

    def slice(s: Rep[Int], e: Rep[Int]) = value.slice(s, e)

    def free = value.free
  }

  def ListToArr(l: ListBuffer[String]): Rep[Array[String]] = {
    val arr = NewArray[String](l.size)
    for (i <- 0 until l.size: Range) {
      arr(i) = l(i)
    }
    arr
  }

  def sort[T: Manifest]
  (arr: RepArray[T], compare: (Rep[T], Rep[T]) => Rep[Int]): RepArray[T] = {
    for (i <- 0 until (arr.length - 1)) {
      var min_idx = i
      for (j <- (i + 1) until arr.length) {
        if (compare(arr(j), arr(min_idx)) < 0) min_idx = j
      }
      val temp = arr(min_idx)
      arr(min_idx) = arr(i)
      arr(i) = temp
    }
    arr
  }

  def merge[T: Manifest](arr: RepArray[T], ends: RepArray[Int], compare: (Rep[T], Rep[T]) => Rep[Int]): RepArray[T] = {
    val starts = NewArray[Int](ends.length)
    starts(0) = 0
    for (i <- 0 until ends.length - 1) {
      starts(i+1) = ends(i)
    }
    while (starts)
  }

  def compare(str1: Rep[String], str2: Rep[String]): Rep[Int] = {
    val l1 = str1.length
    val l2 = str2.length
    val lmin = if (l2 < l1) l2 else l1

    for (i <- 0 until lmin) {
      val str1_ch = str1.charAt(i)
      val str2_ch = str2.charAt(i)
      if (str1_ch != str2_ch) return str1_ch - str2_ch
    }

    // Edge case for strings like
    // String 1="Geeks" and String 2="Geeksforgeeks"
    if (l1 < l2) l1 - l2
    else { // If none of the above conditions is true,
      // it implies both the strings are equal
      0
    }
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
                line.substring(line.indexOfSlice("DatanodeInfoWithStorage") - 1, line.length)
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
trait MapReduceOps extends FileOps with ScannerOps with HDFSOps with LMSMore {

  abstract class MapReduceComputation[MapperResult: Manifest, ReducerResult: Manifest] {

    def Mapper(buf: RepArray[Char]): RepArray[MapperResult]

    def Reducer(l: RepArray[MapperResult]): Rep[ReducerResult]
  }

  def HDFSExec[MapperResult: Manifest, ReducerResult: Manifest](
                                                                 filename: String,
                                                                 mapReduce: MapReduceComputation[MapperResult, ReducerResult],
                                                                 compare: (Rep[MapperResult], Rep[MapperResult]) => Rep[Int]) = {
    val paths = ListToArr(GetPaths(filename))
    val buf = NewArray[Char](GetBlockLen() + 1)

    // Run first mapper independently to calculate length of final arr
    val block_num_f = open(paths(0))
    val size_f = filelen(block_num_f)
    val readbuf = readFile(block_num_f, buf, size_f)
    val foo = mapReduce.Mapper(readbuf)
    val arr_f = sort(foo, compare)

    val mapperout = NewArray[MapperResult](paths.length * arr_f.length * 2)
    val ends = NewArray[Int](paths.length) // Array storing last indices of each mapper result in arr
    for (i <- 0 until arr_f.length) {
      mapperout(i) = arr_f(i)
    }
    ends(0) = arr_f.length

    for (i <- 1 until paths.length) {
      val block_num = open(paths(i))
      val size = filelen(block_num)
      val out = readFile(block_num, buf, size)
      val foo = mapReduce.Mapper(out)
      val arr = sort(foo, compare)

      for (j <- 0 until arr.length) {
        mapperout(ends(i - 1) + j) = arr(j)
      }
      ends(i) = arr.length
    }

    val msort_arr = merge(RepArray(mapperout, ends(paths.length - 1)), RepArray(ends, paths.length), compare)
    var idx_start = 0
    val logical_end = ends(paths.length - 1)
    while (idx_start < logical_end) {
      var idx_end = idx_start + 1
      val key = msort_arr(idx_start)
      while (msort_arr(idx_end) == key && idx_end <= logical_end) idx_end = idx_end + 1
      println(key)
      println(mapReduce.Reducer(RepArray(msort_arr.slice(idx_start, idx_end), idx_end - idx_start)))
      idx_start = idx_end
    }
  }
}

trait MyFoo extends MapReduceOps with ArrayOps {
  @virtualize
  case class MyComputation() extends MapReduceComputation[String, Int] {

    override def Mapper(buf: RepArray[Char]): RepArray[String] = {
      val arr = NewArray[String](buf.length)
      var start = 0
      var count = 0
      while (start < (buf.length - 1)) {
        while (buf(start) == ' ' || buf(start) == '\n' && start < (buf.length - 1)) start = start + 1
        var end = start + 1
        while ((buf(end) != ' ' || buf(end) != '\n') && (end < buf.length)) end = end + 1
        val word = buf.slice(start, end).ArrayOfCharToString()
        arr(count) = word
        count = count + 1
      }
      RepArray(arr, count)
    }

    override def Reducer(l: RepArray[String]): Rep[Int] = {
      l.length
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
        val res = HDFSExec("/10G.txt", MyComputation(), compare)
      }
    }
    println(snippet.code)
  }
}
