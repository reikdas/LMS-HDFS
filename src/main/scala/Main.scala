import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{CCodeGenLibFunction, LibFunction, ScannerOps}
import lms.collection.mutable.ArrayOps

import java.io.File
import scala.collection.mutable.ListBuffer
import sys.process._

trait FileOps extends ScannerOps with LMSMore {
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
trait LMSMore extends ArrayOps with LibFunction {

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

  case class RepString(arr: RepArray[Char], start: Rep[Int], length: Rep[Int])

  def getString(rstring: RepString, tmp: Rep[Array[Char]]) = {
    unchecked[Unit](tmp, "[", rstring.length, "] = '\\0'")
    unchecked[String]("strncpy(", tmp, ",", rstring.arr.slice(rstring.start, rstring.start + rstring.length), ", ", rstring.length, ")")
  }

  def isspace(c: Rep[Char]) = libFunction[Boolean]("isspace", Unwrap(c))(Seq[Int](0), Seq(), Set())

  def strcmp(str1: Rep[String], str2: Rep[String]) = libFunction[Int]("strcmp", Unwrap(str1), Unwrap(str2))(Seq[Int](0, 1), Seq(), Set())
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
trait MapReduceOps extends HDFSOps with FileOps {

  def Mapper(buf: RepArray[Char]) = {
    val arr = NewArray[String](buf.length)
    var start = 0
    var count = 0
    while (start < (buf.length - 1)) {
      while (isspace(buf(start)) && start < (buf.length - 1)) start = start + 1
      var end = start + 1
      while (!isspace(buf(end)) && (end < buf.length)) end = end + 1
      val word = RepString(buf, start, end - start)
      val tmp = NewArray[Char](end-start + 1)
      arr(count) = getString(word, tmp)
      start = end
      count = count + 1
    }
    RepArray(arr, count)
  }

  def HDFSExec(filename: String) = {
    val paths = ListToArr(GetPaths(filename))
    val buf = NewArray[Char](GetBlockLen() + 1)
    val mapperout = NewArray[String](GetBlockLen()*paths.length + 1)

    var end = 0

    for (i <- 0 until paths.length) {
      val block_num = open(paths(i))
      val size = filelen(block_num)
      val out = readFile(block_num, buf, size)
      val arr = Mapper(out)

      for (j <- 0 until arr.length) {
        mapperout(end) = arr(j)
        end = end + 1
      }
    }

    var idx = 0
    while (idx < end) {
      val k = mapperout(idx)
      if (strcmp(k, "") != 0) {
        var count = 0
        var j = idx + 1
        while (j < end) {
          if (strcmp(mapperout(j), k) == 0) {
            mapperout(j) = ""
            count = count + 1
          }
          j = j + 1
        }
        printf("%s = %d\n", k, count)
      }
      idx += 1
    }
    buf.free
    mapperout.free
  }
}

object Main {

  def main(args: Array[String]): Unit = {
    val snippet = new DslDriverC[Int, Unit] with MapReduceOps {
      q =>
      override val codegen = new DslGenC with CCodeGenLibFunction {
        registerHeader("\"scanner_header.h\"")
        registerHeader("<ctype.h>")
        registerHeader("<string.h>")
        val IR: q.type = q
      }

      @virtualize
      def snippet(dummy: Rep[Int]) = {
        val res = HDFSExec("/1G.txt")
      }
    }
    println(snippet.code)
  }
}
