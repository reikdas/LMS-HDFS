import org.scalatest.FunSuite

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.sys.process._

// Ensure Hadoop is running in the background
class TestWS extends FunSuite with Utils {

  val ops = new WhitespaceOps()
  val benchFlag = false
  val printFlag = true
  val execname = "src/test/resources/testws"
  val outcountpath = "src/test/resources/testws.txt"
  val lms_path = sys.props.get("LMS_PATH").get
  val lmshdfs_path = new File(".").getCanonicalPath
  val includeFlags = "-I %s/src/main/resources/headers/ -I %s/src/main/resources/headers/".format(lmshdfs_path, lms_path)

  test("Whitespace 1G: num_blocks = num_procs") {
    val outcodepath = "src/test/resources/testws.c"
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
    val execcomm = "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname)
    val count: String = execcomm.!!
    assert(count.trim().equals("161390000"))
    cleanup(filesToDelete.toList)
  }

  test("Whitespace 1G: num_blocks/2 = num_procs") {
    val outcodepath = "src/test/resources/testws.c"
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
    val execcomm = "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname)
    val count = execcomm.!!
    assert(count.trim().equals("161390000"))
    cleanup(filesToDelete.toList)
  }

  test("Whitespace 1G: num_blocks%num_procs != 0") {
    val outcodepath = "src/test/resources/testws.c"
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
    val execcomm = "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname)
    val count = execcomm.!!
    assert(count.trim().equals("161390000"))
    cleanup(filesToDelete.toList)
  }

  test("Whitespace 1G: num_procs == 1") {
    val outcodepath = "src/test/resources/testws.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 1
    val execcomm = "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname)
    val count = execcomm.!!
    assert(count.trim().equals("161390000"))
    cleanup(filesToDelete.toList)
  }
}
