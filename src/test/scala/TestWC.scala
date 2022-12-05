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

  def makeDriver(filepath: String) = new DslDriverC[Int, Unit] with MapReduceOps {
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
      val paths = GetPaths(filepath)
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

  val execname = "src/test/resources/testwc"
  val outcountpath = "src/test/resources/testwc.txt"
  val lms_path = sys.props.get("LMS_PATH").get
  val lmshdfs_path = new File(".").getCanonicalPath
  val includeFlags = "-I %s/src/main/resources/headers/ -I %s/src/main/resources/headers/".format(lmshdfs_path, lms_path)

  test("Wordcount 1G: num_blocks = num_procs") {
    val outcodepath = "src/test/resources/testwc.c"
    makeDriver("/1G.txt").emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 8
    "mpirun -np %s %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G: num_blocks/2 = num_procs") {
    val outcodepath = "src/test/resources/testwc.c"
    makeDriver("/1G.txt").emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 4
    "mpirun -np %s %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G: num_blocks%num_procs != 0") {
    val outcodepath = "src/test/resources/testwc.c"
    makeDriver("/1G.txt").emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 3
    "mpirun -np %s %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G: num_procs == 1") {
    val outcodepath = "src/test/resources/testwc.c"
    makeDriver("/1G.txt").emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 3
    "mpirun -np %s %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Word split at boundary") {
    val outcodepath = "src/test/resources/testwc.c"
    makeDriver("/text.txt").emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 4
    "mpirun -np %s %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wctext.txt")))
    //cleanup(filesToDelete.toList)
  }
}
