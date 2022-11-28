import lms.core.stub._
import lms.core.virtualize
import lms.thirdparty.{CCodeGenCMacro, CCodeGenLibFunction, CCodeGenMPI, CCodeGenScannerOps}
import org.scalatest.FunSuite

import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths}
import java.util
import scala.collection.mutable.ListBuffer
import scala.sys.process._

// Ensure Hadoop is running in the background
class TestWC extends FunSuite {

  val snippet = new DslDriverC[Int, Unit] with MapReduceOps {
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

      registerHeader("<ctype.h>")
      registerHeader("src/main/resources/headers", "\"ht.h\"")
      val IR: q.type = q
    }

    @virtualize
    def snippet(dummy: Rep[Int]) = {
      val paths = GetPaths("/1G.txt")
      HDFSExec(paths)
      paths.free
    }

    def emitMyCode(outpath: String) = {
      codegen.emitSource[Int, Unit](wrapper, "Snippet", new java.io.PrintStream(outpath))
    }
  }

  def cleanup(files: List[String]) = {
    for (file <- files) {
      new File(file).delete()
    }
  }

  private def isEqual(firstFile: Path, secondFile: Path): Boolean = {
    try {
      if (Files.size(firstFile) != Files.size(secondFile)) return false
      val first = Files.readAllBytes(firstFile)
      val second = Files.readAllBytes(secondFile)
      return util.Arrays.equals(first, second)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    false
  }

  val outcodepath = "src/test/resources/testwc.c"
  snippet.emitMyCode(outcodepath)

  // FIXME: Don't hardcode paths
  test("Test WC") {
    val filesToDelete = new ListBuffer[String]()
    val execname = "src/test/resources/testwc"
    val outcountpath = "src/test/resources/testwc.txt"
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val includeFlags = "-I /home/reikdas/lmshdfs/src/main/resources/headers/ -I /home/reikdas/Research/lms-clean/src/main/resources/headers/"
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 8
    "mpirun -np %s %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }
}
