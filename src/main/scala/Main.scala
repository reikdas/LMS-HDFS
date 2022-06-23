import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize

import sys.process._
import scala.collection.mutable.ListBuffer
import scala.io.StdIn.readLine

def GetPath(path: String): Unit = {
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
    if (line.size > 0 && count == 0) {
      if (words(0).equals(path)) {
        count = count + 1
        size = words(1).toInt
        flag = false
      } else {
        flag = true
      }
    }
    if (flag == true && line.size > 0 && count > 0) {
      if (words(0).size > 0) {
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
  for (i <- 0 until blocks_infos.size) {
    println("current/%s/current/finalized/subdir0/subdir0/%s".format(dnodes_infos(i), blocks_infos(i)))
    println("IP = ", ips_infos(i))
  }
}

object Main extends App {
  val filename = readLine()
  GetPath("/tpc-h/lineitem.tbl")
}

/*@virtualize
object Main extends App {
  val snippet = new DslDriverC[Int, Int] {
    def snippet(x: Rep[Int]) = {
      def compute(b: Rep[Boolean]): Rep[Int] = {
        if (b) 1 else x
      }
      compute(x==1)
    }
  }
  println("Evaluated result = ", snippet.eval(0))
  println(snippet.code)
}*/
