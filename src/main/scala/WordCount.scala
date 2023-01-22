import lms.core.stub._
import lms.macros.SourceContext
import lms.core.virtualize

@virtualize
class WordCountOps extends DDLoader {
  override def HDFSExec(paths: Rep[Array[String]], readFunc: (Rep[Int], Rep[LongArray[Char]], Rep[Long]) => RepArray[Char], benchFlag: Boolean, printFlag: Boolean) = {
    // MPI initialize
    var world_size = 0
    var world_rank = 0

    val start = timestamp
    Adapter.g.reflectWrite("printflag", Unwrap(start))(Adapter.CTRL)
    mpi_init()
    mpi_comm_size(mpi_comm_world, world_size)
    mpi_comm_rank(mpi_comm_world, world_rank)

    if (world_rank < paths.length) {
      val buf = NewLongArray[Char](GetBlockLen() + 1, Some(0)) // Underlying buffer for readFile
      val idxmap = ht_create()
      val word = NewLongArray[Char](GetBlockLen())
      var total_len = 0
      var word_count = 0
      val blocks_per_proc = (paths.length + world_size - 1) / world_size // Ceil of paths.length/world_size
      val allwords = NewLongArray[Char](GetBlockLen() * blocks_per_proc, Some(0))
      // There can be atmost (block_len/2) + 1 words in a block
      val allvals = NewLongArray[Int](GetBlockLen() * blocks_per_proc / 2)

      // New variables for handling split words
      val buf2 = NewLongArray[Char](GetBlockLen() + 1, Some(0)) // Buffer to hold characters from second file (only ReadFile)
      val buf3 = NewLongArray[Char](GetBlockLen() + 1, Some(0)) // Buffer to hold characters from next process
      var tmpbuf4 = NewLongArray[Char](GetBlockLen() * 2, Some(0)) // Temporary combined buffer
      var block_num2: Var[Int] = 0

      var nextIsSplit: Var[Boolean] = false
      val last_working_proc = (paths.length / blocks_per_proc) - 1
      for (i <- 0 until blocks_per_proc) {
        val idx = (world_rank * blocks_per_proc) + i
        if (idx < paths.length) { // Since blocks_per_proc is the upper bound
          val block_num: Rep[Int] = if (nextIsSplit == true) {
            block_num2
          } else {
            open(paths(idx))
          }

          val fpointer: RepArray[Char] = readFunc(block_num, buf, filelen(block_num)) // Do I need to open file if already open?
          var start: Var[Long] = 0L

          if (i==0 && world_rank != 0) {
            while ((start < fpointer.length) && !isspace(fpointer((start)))) start = start + 1L
            start = start + 1L
            mpi_send(start, 1, mpi_long, world_rank - 1, 0, mpi_comm_world)
            mpi_send(fpointer.value(0L), start.toInt, mpi_char, world_rank - 1, 1, mpi_comm_world)
          } else if (nextIsSplit == true) { // If first word is a split word, skip it
            while ((start < fpointer.length) && !isspace(fpointer((start)))) start = start + 1L
          }

          val len_rec = NewArray0[Long](1)
          if ((i==blocks_per_proc - 1) && (world_rank != last_working_proc)) {
            mpi_rec(len_rec(0), 1, mpi_long, world_rank + 1, 0, mpi_comm_world)
            mpi_rec2(buf3, len_rec(0).toInt, mpi_char, world_rank + 1, 1, mpi_comm_world)
            memcpy3(tmpbuf4, fpointer.value(0L), fpointer.length)
            memcpy3(tmpbuf4.slice(fpointer.length, -1L), buf3(0L), len_rec(0))
          } else {
            tmpbuf4 = fpointer.value
          }

          val buf4 = RepArray(tmpbuf4, fpointer.length + len_rec(0).toInt)

          while (start < buf4.length) {
            while (start < (buf4.length) && isspace(buf4(start))) start = start + 1L
            if (start < buf4.length) {
              var end: Var[Long] = start + 1L
              while ((end < buf4.length) && !isspace(buf4(end))) end = end + 1L
              var off: Var[Long] = 0L
              nextIsSplit = false
              if (end.toInt == buf4.length) {
                if (!isspace(buf4(end - 1)) && (idx < paths.length - 1) && !(i==blocks_per_proc - 1)) { // Check if last word is split
                  nextIsSplit = true
                } else {
                  off = 1L
                }
              }
              var len: Var[Long] = end - start - off
              if (nextIsSplit == true) {
                block_num2 = open(paths(idx + 1))
                val fpointer2 = readFunc(block_num2, buf2, filelen(block_num2))
                var newstart: Var[Long] = 0L
                while ((newstart < fpointer2.length) && !isspace(fpointer2(newstart))) newstart = newstart + 1L
                len = len + newstart
                var arrcounter = 0L
                var j: Var[Long] = start
                while (j < end) {
                  word(arrcounter) = buf4(j)
                  j = j + 1L
                  arrcounter = arrcounter + 1L
                }
                j = 0L
                while (j < newstart) {
                  word(arrcounter) = fpointer2(j)
                  j = j + 1L
                  arrcounter = arrcounter + 1L
                }
              } else {
                memcpy2(word, buf4.slice(start, -1L), len)
              }
              word(len) = '\0'
              val value = ht_get(idxmap, word)
              if (value == -1L) {
                ht_set(idxmap, word, word_count.toLong)
                memcpy2(allwords.slice(total_len.toLong, -1L), word, len)
                allwords(total_len + len) = '\0'
                total_len = total_len + len.toInt + 1
                allvals(word_count.toLong) = 1
                word_count = word_count + 1
              } else {
                allvals(value) = allvals(value) + 1
              }
              start = end
            }
          }
          close(block_num)
        }
      }

      val recv_data = NewLongArray[Int](world_size.toLong, Some(0))
      mpi_allgather2(total_len, 1, mpi_int, recv_data, 1, mpi_int, mpi_comm_world)
      val num_words = NewLongArray[Int](world_size.toLong, Some(0))
      mpi_allgather2(word_count, 1, mpi_int, num_words, 1, mpi_int, mpi_comm_world)
      val displs = NewArray0[Int](world_size)
      displs(0) = 0
      for (i <- 1 until world_size) {
        displs(i) = displs(i - 1) + recv_data(i - 1)
      }
      val displs2 = NewArray0[Int](world_size)
      displs2(0) = 0
      for (i <- 1 until world_size) {
        displs2(i) = displs2(i - 1) + num_words(i - 1)
      }
      var fullarr_len = 0L
      var fullvalarr_len = 0L
      if (world_rank == 0) {
        for (i <- 0 until world_size) {
          fullarr_len = fullarr_len + recv_data(i)
          fullvalarr_len = fullvalarr_len + num_words(i)
        }
      }
      val fullarr: Rep[LongArray[Char]] = if (world_rank == 0) NewLongArray[Char](fullarr_len) else `null`[LongArray[Char]]
      val fullvalarr: Rep[LongArray[Int]] = if (world_rank == 0) NewLongArray[Int](fullvalarr_len) else `null`[LongArray[Int]]
      mpi_gatherv(allwords, total_len, mpi_char, fullarr, recv_data, displs, mpi_char, 0, mpi_comm_world)
      mpi_gatherv(allvals, word_count, mpi_int, fullvalarr, num_words, displs2, mpi_int, 0, mpi_comm_world)
      ht_destroy(idxmap)
      if (world_rank == 0) {
        val hmap = ht_create()
        var counter = 0L
        var word_idx: Var[Long] = 0L
        while (word_idx < fullvalarr_len) {
          val len = strlen(fullarr.slice(counter, -1L))
          val value = ht_get(hmap, fullarr.slice(counter, -1L))
          if (value == -1L) {
            ht_set(hmap, fullarr.slice(counter, -1L), fullvalarr(word_idx).toLong)
          } else {
            ht_set(hmap, fullarr.slice(counter, -1L), value + fullvalarr(word_idx).toLong)
          }

          word_idx = word_idx + 1L
          counter = counter + len + 1L
        }
        val it = ht_iterator(hmap)
        if (printFlag) {
          while (ht_next(it)) {
            printf("%s %ld\n", hti_key(it), hti_value(it))
          }
        } else {
          Adapter.g.reflectWrite("printflag", Unwrap(it))(Adapter.CTRL)
        }
        ht_destroy(hmap)
      }
      fullarr.free
      fullvalarr.free
      displs.free
      displs2.free
      num_words.free
      recv_data.free
      allvals.free
      allwords.free
      word.free
      buf.free
      buf2.free
//      buf3.free
//      tmpbuf4.free
    }
    mpi_finalize()
    if (benchFlag) {
      val end = timestamp
      Adapter.g.reflectWrite("printflag", Unwrap(end))(Adapter.CTRL)
      printf("Proc %d spent %ld time.\n", world_rank, end - start)
    }
    paths.free
  }
}

object WordCount extends ArgParser {

  def main(args: Array[String]): Unit = {
    val ops = new WordCountOps()
    val options = parseargs(args)
    val loadFile = options.getOrElse("loadFile", throw new RuntimeException("No load file")).toString
    val writeFile = options.getOrElse("writeFile", throw new RuntimeException("No write file")).toString
    val benchFlag = if (options.exists(_._1 == "bench")) {
      options("bench").toString.toBoolean
    } else {
      false
    }
    val printFlag = if (options.exists(_._1 == "print")) {
      options("print").toString.toBoolean
    } else {
      false
    }
    val mmapFlag = if (options.exists(_._1 == "mmap")) {
      true
    } else {
      false
    }
    val driver = new DDLDriver(ops, loadFile, mmapFlag, benchFlag, printFlag) {}
    driver.emitMyCode(writeFile)
  }
}
