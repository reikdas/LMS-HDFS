import lms.core.stub._
import lms.macros.SourceContext
import lms.core.Backend._
import lms.core.{Backend, virtualize}
import lms.thirdparty.{CCodeGenCMacro, CCodeGenLibFunction, CCodeGenMPI, CCodeGenScannerOps, LibFunction, MPIOps, ScannerOps}
import lms.collection.mutable.ArrayOps

import java.io.File
import scala.collection.mutable.ListBuffer
import sys.process._

trait FileOps extends LMSMore {
  def readFile(fd: Rep[Int], buf: Rep[LongArray[Char]], size: Rep[Long]): RepArray[Char] = {
    val readlen = libFunction[Int]("read", Unwrap(fd), Unwrap(buf), Unwrap(size))(Seq[Int](), Seq(1), Set())
    RepArray[Char](buf, readlen)
  }

  def mmapFile(fd: Rep[Int], buf: Rep[LongArray[Char]], size: Rep[Long]): RepArray[Char] = {
    val newbuf = mmap2[Char](fd, size)
    RepArray[Char](newbuf, size.toInt)
  }
}

@virtualize
trait HashMapOps extends LMSMore {
  def ht_create() = unchecked[Array[ht]]("ht_create()")

  def ht_destroy(tab: Rep[Array[ht]]) = libFunction[Array[ht]]("ht_destroy", Unwrap(tab))(Seq(0), Seq(), Set())

  def ht_get(tab: Rep[Array[ht]], arr: Rep[LongArray[Char]]) = {
    val effectkey = arr match {
      case EffectView(x, base) => base
      case _ => arr
    }
    libFunction2[Long]("ht_get", Unwrap(tab), Unwrap(arr))(Seq(Unwrap(tab), Unwrap(effectkey)), Seq(), Set())
  }

  def ht_set(tab: Rep[Array[ht]], arr: Rep[LongArray[Char]], value: Rep[Long]) = {
    val effectkey = arr match {
      case EffectView(x, base) => base
      case _ => arr
    }
    libFunction2[Unit]("ht_set", Unwrap(tab), Unwrap(arr), Unwrap(value))(Seq(Unwrap(tab), Unwrap(effectkey)), Seq(Unwrap(tab)), Set())
  }

  def ht_iterator(tab: Rep[Array[ht]]) = libFunction[hti]("ht_iterator", Unwrap(tab))(Seq(0), Seq(), Set())

  def ht_next(iter: Rep[hti]) = libFunction[Boolean]("ht_next", Unwrap(iter))(Seq(0), Seq(0), Set(0))

  def hti_value(iter: Rep[hti]) = libFunction[Long]("hti_value", Unwrap(iter))(Seq(0), Seq(), Set(0))

  def hti_key(iter: Rep[hti]) = libFunction[Array[Char]]("hti_key", Unwrap(iter))(Seq(0), Seq(), Set(0))

  class ht

  class hti
}

@virtualize
trait CharArrayOps extends LMSMore with OrderingOps {
  def isspace(c: Rep[Char]) = libFunction[Boolean]("isspace", Unwrap(c))(Seq[Int](0), Seq(), Set())

  def strncpy(str1: Rep[Array[Char]], str2: Rep[Array[Char]], length: Int) =
    libFunction[Array[Char]]("strncpy", Unwrap(str1), Unwrap(str2), Unwrap(length))(Seq[Int](0, 1, 2), Seq(0), Set())

  def hashCode(str: Rep[LongArray[Char]], len: Rep[Long]) = {
    var hashVal = 0L
    var i = 0L
    while (i < len) {
      val off = str(i).toInt
      if (off < 0) {
        hashVal = 0L
      } else {
        hashVal = off + ((31L * hashVal) % (2L << 32L))
      }
      i += 1
    }
    hashVal
  }

  def strlen(arr: Rep[LongArray[Char]]) = {
    val effectkey = arr match {
      case EffectView(x, base) => base
      case _ => arr
    }
    libFunction2[Int]("strlen", Unwrap(arr))(Seq(Unwrap(effectkey)), Seq(), Set())
  }

  case class RepString(arr: RepArray[Char], start: Rep[Int], length: Rep[Int]) {
    def apply(idx: Rep[Int]) = arr(idx)

    def update(idx: Rep[Int], something: Rep[Char]): Unit = {
      arr(idx) = something
    }
  }
}

trait MyMPIOps extends LibFunction with ArrayOps with MPIOps {
  def mpi_send[T: Manifest](data: Rep[T], count: Rep[Int], datatype: Rep[MPIDataType], dest: Rep[Int], tag: Rep[Int], world: Rep[MPIComm]) =
    unchecked[Unit]("MPI_Send(&", data, ", ", count, ",", datatype, ", ", dest, ", ", tag, ", ", world, ")")

  def mpi_rec[T: Manifest](data: Rep[T], count: Rep[Int], datatype: Rep[MPIDataType], source: Rep[Int], tag: Rep[Int], world: Rep[MPIComm]) =
    unchecked[Unit]("MPI_Recv(&", data, ", ", count, ",", datatype, ", ", source, ", ", tag, ", ", world, ", MPI_STATUS_IGNORE)")

  def mpi_allgather(sendbuf: Rep[LongArray[Long]], sendcount: Rep[Long], sendtype: Rep[MPIDataType], recvbuf: Rep[LongArray[Long]],
                    recvcount: Rep[Long], recvtype: Rep[MPIDataType], comm: Rep[MPIComm]) =
    libFunction[Unit]("MPI_Allgather", Unwrap(sendbuf), Unwrap(sendcount), Unwrap(sendtype), Unwrap(recvbuf), Unwrap(recvcount),
      Unwrap(recvtype), Unwrap(comm))(Seq(0, 1, 2, 3, 4, 5, 6), Seq(3), Set())

  def mpi_allgather2(sendbuf: Rep[Int], sendcount: Rep[Int], sendtype: Rep[MPIDataType], recvbuf: Rep[LongArray[Int]],
                     recvcount: Rep[Int], recvtype: Rep[MPIDataType], comm: Rep[MPIComm]) =
    libFunction[Unit]("MPI_Allgather", Unwrap(sendbuf), Unwrap(sendcount), Unwrap(sendtype), Unwrap(recvbuf), Unwrap(recvcount),
      Unwrap(recvtype), Unwrap(comm))(Seq(0, 1, 2, 3, 4, 5, 6), Seq(3), Set(0))

  def mpi_gatherv[T: Manifest](sendbuf: Rep[LongArray[T]], sendcount: Rep[Int], sendtype: Rep[MPIDataType], recvbuf: Rep[LongArray[T]],
                               recvcounts: Rep[LongArray[Int]], displs: Rep[Array[Int]], recvtype: Rep[MPIDataType], root: Rep[Int], comm: Rep[MPIComm]) = {
    val effectkey = recvbuf match {
      case EffectView(x, base) => base
      case _ => recvbuf
    }
    libFunction[Unit]("MPI_Gatherv", Unwrap(sendbuf), Unwrap(sendcount), Unwrap(sendtype), Unwrap(recvbuf), Unwrap(recvcounts),
      Unwrap(displs), Unwrap(recvtype), Unwrap(root), Unwrap(comm))(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8), Seq(3), Set(), Unwrap(effectkey))
  }

  def mpi_reduce(send_data: Rep[Long], recv_data: Var[Long], count: Rep[Int], datatype: Rep[MPIDataType], op: Rep[MPIOp],
                 root: Rep[Int], comm: Rep[MPIComm]) = {
    unchecked[Unit]("MPI_Reduce(&", send_data, ", &", recv_data, ",", count, ",", datatype, ",", op, ",", root, ",", comm, ")")
  }

  def mpi_reduce2(send_data: Rep[Array[Long]], recv_data: Rep[Array[Long]], count: Rep[Int], datatype: Rep[MPIDataType], op: Rep[MPIOp],
                  root: Rep[Int], comm: Rep[MPIComm]) = {
    unchecked[Unit]("MPI_Reduce(", send_data, ", ", recv_data, ",", count, ",", datatype, ",", op, ",", root, ",", comm, ")")
  }

  def mpi_wtime() = unchecked[Double]("MPI_Wtime()")
}

@virtualize
trait LMSMore extends ArrayOps with LibFunction with ScannerOps {

  def ListToArr(l: ListBuffer[String]): Rep[Array[String]] = {
    val arr = NewArray[String](l.size)
    for (i <- 0 until l.size: Range) {
      arr(i) = l(i)
    }
    arr
  }

  def NewArray0[T: Manifest](x: Rep[Int]): Rep[Array[T]] = {
    Wrap[Array[T]](Adapter.g.reflectMutable("NewArray", Unwrap(x), Backend.Const(0)))
  }

  def `null`[T: Manifest]: Rep[T] = Wrap[T](Backend.Const(null))

  def mmap2[T: Manifest](fd: Rep[Int], len: Rep[Long]) = (libFunction[LongArray[T]]("mmap",
    lms.core.Backend.Const(0), Unwrap(len), Unwrap(prot), Unwrap(fd), lms.core.Backend.Const(0))(Seq[Int](), Seq[Int](), Set[Int]()))

  def memcpy2[T: Manifest](destination: Rep[LongArray[T]], source: Rep[LongArray[T]], num: Rep[Long]) = {
    val desteffectkey = destination match {
      case EffectView(x, base) => base
      case _ => destination
    }
    val srceffectkey = source match {
      case EffectView(x, base) => base
      case _ => source
    }
    libFunction2[Unit]("memcpy", Unwrap(destination), Unwrap(source), Unwrap(num))(Seq(Unwrap(srceffectkey)), Seq(Unwrap(desteffectkey)), Set())
  }

  def libFunction2[T: Manifest](m: String, rhs: lms.core.Backend.Exp*)(rkeys: Seq[lms.core.Backend.Exp], wkeys: Seq[lms.core.Backend.Exp], pkeys: Set[Int]): Rep[T] = {
    val defs = Seq(lms.core.Backend.Const(m), lms.core.Backend.Const(pkeys)) ++ rhs
    Wrap[T](Adapter.g.reflectEffect("lib-function", defs: _*)(rkeys: _*)(wkeys: _*))
  }

  case class RepArray[T: Manifest](value: Rep[LongArray[T]], length: Rep[Int]) {
    def apply(idx: Rep[Long]): Rep[T] = value(idx)

    def update(idx: Rep[Long], something: Rep[T]): Unit = {
      value(idx) = something
    }

    def slice(s: Rep[Long], e: Rep[Long]) = value.slice(s, e)

    def free = value.free
  }
}

trait HDFSOps extends LMSMore {
  def GetPaths(path: String) = {
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
    ListToArr(nativepaths)
  }

  def GetBlockLen(): Long = {
    val output = "hdfs getconf -confKey dfs.blocksize".!!
    output.replace("\n", "").toLong
  }
}

trait DDLoader extends HDFSOps with FileOps with MyMPIOps with CharArrayOps with HashMapOps with LMSMore {
  def HDFSExec(paths: Rep[Array[String]], readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char], benchFlag: Boolean, printFlag: Boolean): Unit
}

class DDLDriver(plugin: DDLoader, loadFile: String, mmapFlag: Boolean, benchFlag: Boolean, printFlag: Boolean) extends DslDriverC[Int, Unit] with HDFSOps with HashMapOps with FileOps {
  q =>
  override val codegen = new DslGenC with CCodeGenLibFunction with CCodeGenMPI with CCodeGenCMacro with CCodeGenScannerOps {
    override def remap(m: Typ[_]): String =
      if (m <:< manifest[ht]) {
        "ht"
      } else if (m <:< manifest[hti]) {
        "hti"
      } else {
        super.remap(m)
      }

    override def traverse(n: Node): Unit = n match {
      case n@Node(_, "printflag", _, _) =>
      case _ => super.traverse(n)
    }

    registerHeader("<ctype.h>")
    registerHeader("src/main/resources/headers", "\"ht.h\"")
    val IR: q.type = q
  }

  @virtualize
  def snippet(dummy: Rep[Int]) = {
    val paths = GetPaths(loadFile).asInstanceOf[plugin.Rep[Array[String]]]
    val readFunc = if (mmapFlag) {
      mmapFile _
    } else {
      readFile _
    }
    plugin.HDFSExec(paths, readFunc.asInstanceOf[(plugin.Rep[Int], plugin.Rep[plugin.LongArray[Char]], plugin.Rep[Long]) => plugin.RepArray[Char]], benchFlag, printFlag)
  }

  def emitMyCode(writeFile: String) = {
    codegen.emitSource[Int, Unit](wrapper, "Snippet", new java.io.PrintStream(writeFile))
  }
}
