import org.scalatest.FunSuite

import java.io.File
import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import scala.sys.process._

// Ensure Hadoop is running in the background
class TestWC extends FunSuite with Utils {

  val ops = new WordCountOps()
  val benchFlag = false
  val printFlag = true
  val execname = "src/test/resources/testwc"
  val outcountpath = "src/test/resources/testwc.txt"
  val lms_path = sys.props.get("LMS_PATH").get
  val lmshdfs_path = new File(".").getCanonicalPath
  val includeFlags = "-I %s/src/main/resources/headers/ -I %s/src/main/resources/headers/".format(lmshdfs_path, lms_path)

  test("Wordcount 1G: num_blocks = num_procs") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag, true) {}
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
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G: num_blocks/2 = num_procs") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag, true) {}
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
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G: num_blocks%num_procs != 0") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag, true) {}
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
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G: num_procs == 1") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/1G.txt", true, benchFlag, printFlag, true) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 1
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G: read") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/1G.txt", false, benchFlag, printFlag, true) {}
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
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wc1G.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G Word split at boundary: num_blocks = num_procs") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/text.txt", true, benchFlag, printFlag, true) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 6
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wctext.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G Word split at boundary: num_blocks/2 = num_procs") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/text.txt", true, benchFlag, printFlag, true) {}
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
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wctext.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G Word split at boundary: num_blocks%num_procs != 0") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/text.txt", true, benchFlag, printFlag, true) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 5
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wctext.txt")))
    cleanup(filesToDelete.toList)
  }

  test("Wordcount 1G Word split at boundary: num_blocks == 1") {
    val outcodepath = "src/test/resources/testwc.c"
    val driver = new DDLDriver(ops, "/text.txt", true, benchFlag, printFlag, true) {}
    driver.emitMyCode(outcodepath)
    val filesToDelete = new ListBuffer[String]()
    filesToDelete += execname
    filesToDelete += outcountpath
    cleanup(filesToDelete.toList)
    filesToDelete += outcodepath
    val compile = "mpicc %s %s -o %s".format(outcodepath, includeFlags, execname)
    compile.!!
    val nprocs = 1
    "mpirun -np %s --mca btl ^openib %s 0".format(nprocs, execname) #>> new File(outcountpath) !
    val sortcmd = "sort %s -o %s".format(outcountpath, outcountpath)
    sortcmd.!!
    assert(isEqual(Paths.get(outcountpath), Paths.get("src/test/resources/wctext.txt")))
    cleanup(filesToDelete.toList)
  }
}
