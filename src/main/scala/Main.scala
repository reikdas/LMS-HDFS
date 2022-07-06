import lms.collection.mutable.ArrayOps
import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize
import lms.thirdparty.{LibFunction, LibStruct}

import sys.process._
import scala.collection.mutable.ListBuffer
import scala.io.StdIn.readLine

trait FileOps extends Base with ArrayOps with LibStruct with LibFunction {
  def open(loc: Rep[String]): Rep[Int] = libFunction[Int]("open", Unwrap(loc), lms.core.Backend.Const(0))(Seq(), Seq(), Set())

  def filelen(fd: Rep[Int])(implicit o: Overloaded1): Rep[Long] = uncheckedPure[Long]("fsize(", fd, ")") // FIXME: Use libFunction instead
  def filelenf(fd: Rep[Int]) = uncheckedPure[Int]("fsize(", fd, ")") // FIXME: Use libFunction instead
  def mmap[T](fd: Rep[Int], len: Rep[Long])(implicit o: Overloaded2, mt: Manifest[T]): Rep[LongArray[T]] = Wrap[LongArray[T]](Adapter.g.reflect("mmap", Unwrap(len), Unwrap(fd)))
  def mmap[T](fd: Rep[Int], len: Rep[Int])(implicit o: Overloaded1, mt: Manifest[T]): Rep[Array[T]] = Wrap[Array[T]](Adapter.g.reflect("mmap", Unwrap(len), Unwrap(fd)))
  def mmapf[T:Manifest](fd: Rep[Int], len: Rep[Int]) = mmap(fd, len)
  def stringFromCharArray(data: Rep[LongArray[Char]], pos: Rep[Long]): Rep[String] = uncheckedPure[String](data," + ",pos) // FIXME: Use libFunction instead

  class hdfsFS
  def connectToHDFS(address: Rep[String], port: Rep[Int]): Rep[hdfsFS] = libFunction[hdfsFS]("hdfsConnect", Unwrap(address), Unwrap(port))(Seq(0, 1), Seq(), Set())
  class hdfsFile
  def HDFSOpenFile(fs: Rep[hdfsFS], path: Rep[String], flag: Rep[Int], bufferSize: Rep[Int], replication: Rep[Int], blockSize: Rep[Int]): Rep[hdfsFile] = libFunction[hdfsFile]("hdfsOpenFile", Unwrap(fs), Unwrap(path), Unwrap(flag), Unwrap(bufferSize), Unwrap(replication), Unwrap(blockSize))(Seq(), Seq(), Set())
  class hdfsAvailable
  def hdfsAvailable(fs: Rep[hdfsFS], readFile: Rep[hdfsFile]): Rep[Long] = libFunction[Long]("hdfsAvailable", Unwrap(fs), Unwrap(readFile))(Seq(), Seq(), Set())
  def malloc[T](availsize: Rep[Long]): Rep[LongArray[Char]] = uncheckedPure[LongArray[Char]]("malloc(sizeof(char)*(", availsize, "+1))") // FIXME: Use libFunction instead
  def memset(buf: Rep[LongArray[Char]]) = unchecked("memset(", buf, ", 0, sizeof(", buf, "))") // FIXME: Use libFunction instead
  def readBytes(fs: Rep[hdfsFS], readFile: Rep[hdfsFile], pos: Rep[Long], avail: Rep[Long], buf: Rep[LongArray[Char]]): Rep[Long] = unchecked[Long]("hdfsPread(", fs, ",", readFile, ",", pos, ", &", buf, "[", pos, "], ", avail, ")") // FIXME: Use libFunction instead
  def disconnectHDFS(handle: Rep[hdfsFS]): Rep[Int] = unchecked("hdfsDisconnect(", handle, ")") // FIXME: Use libFunction instead
  def closeHDFS(handle: Rep[hdfsFS], openFile: Rep[hdfsFile]): Rep[Int] = unchecked("hdfsCloseFile(", handle, ", ", openFile, ")") // FIXME: Use libFunction instead
}

@virtualize
object Main extends App {

  /*def GetPath(path: String): Unit = {
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
          if (words(0).substring(0, words(0).length - 1).forall(Character.isDigit)) {
            if (words(0).substring(0, words(0).length - 1).toInt == count - 1) {
              count = count + 1
              val rawinfo = words(1).split(":")
              blocks_infos += rawinfo(1).substring(0, rawinfo(1).lastIndexOf("_"))
              dnodes_infos += rawinfo(0)
              val stline = line.substring(line.indexOfSlice("DatanodeInfoWithStorage") - 1, line.size)
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
    println("Size = ", size)
    for (i <- blocks_infos.indices) {
      println("current/%s/current/finalized/subdir0/subdir0/%s".format(dnodes_infos(i), blocks_infos(i)))
      println("IP = ", ips_infos(i))
    }
  }*/


  val snippet = new DslDriverC[Int, Unit] {
    // val paths = GetPath(..)
    val paths = List("/foo.txt", "/bar.txt")
    def snippet(dummy: Rep[Int]) = {
      for (i <- 0 until paths.length: Range) println(paths(i))
    }
  }
  println(snippet.code)
}
