import java.io.{File, IOException}
import java.nio.file.{Files, Path}
import java.util

trait Utils {
  def cleanup(files: List[String]) = {
    for (file <- files) {
      new File(file).delete()
    }
  }

  def isEqual(firstFile: Path, secondFile: Path): Boolean = {
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
}
