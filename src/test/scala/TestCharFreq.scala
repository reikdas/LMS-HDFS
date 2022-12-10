import org.scalatest.FunSuite

import java.io.File
import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import scala.sys.process._

// Ensure Hadoop is running in the background
class TestCharFreq extends FunSuite with Utils {

  val ops = new CharFreqOps()
  val benchFlag = false
  val printFlag = true
  val execname = "src/test/resources/testcharfreq"
  val outcountpath = "src/test/resources/testcharfreq.txt"
  val lms_path = sys.props.get("LMS_PATH").get
  val lmshdfs_path = new File(".").getCanonicalPath
  val includeFlags = "-I %s/src/main/resources/headers/ -I %s/src/main/resources/headers/".format(lmshdfs_path, lms_path)

  test("Char Freq 1G: num_blocks = num_procs") {
    val outcodepath = "src/test/resources/testcharfreq.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 8
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val foo = 1 // FIXME: Dummy line required to handle above linesc
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/charfreq1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Char Freq 1G: num_blocks/2 = num_procs") {
    val outcodepath = "src/test/resources/testcharfreq.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 4
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val foo = 1 // FIXME: Dummy line required to handle above line
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/charfreq1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Char Freq 1G: num_blocks%num_procs != 0") {
    val outcodepath = "src/test/resources/testcharfreq.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 3
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val foo = 1 // FIXME: Dummy line required to handle above line
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/charfreq1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Char Freq 1G: num_procs == 1") {
    val outcodepath = "src/test/resources/testcharfreq.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 3
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val foo = 1 // FIXME: Dummy line required to handle above line
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/charfreq1G.txt")))
    cleanup(filesToDelete.toList)
  }
}
