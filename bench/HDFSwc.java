import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import java.util.Arrays;
import java.util.HashMap;
import java.nio.charset.StandardCharsets;

public class HDFSwc {

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    FSDataInputStream in = null;
    try {
      long startTime = System.currentTimeMillis();
      FileSystem fs = FileSystem.get(conf);
      // Input file path
      Path inFile = new Path(args[0]);
      // open and read from file
      in = fs.open(inFile);
      ContentSummary cSummary = fs.getContentSummary(inFile);
      long length = cSummary.getLength();
      long count = 0;
      int buflen = Integer.MAX_VALUE - 2;
      byte[] buffer = new byte[buflen];
      HashMap<String, Long> map = new HashMap<String, Long>();


      long i = 0;
      while (i < length) {
        long diff = length - i;
        int toread = ((long)buflen) > diff? (int)diff : buflen;
        in.readFully(i, buffer, 0, toread);
        int start = 0;
        while (start < toread) {
          while (start < toread && Character.isWhitespace(buffer[start])) {
            start += 1;
          }
          if (start < toread) {
            int end = start + 1;
            while (end < toread && !Character.isWhitespace(buffer[end])) {
              end += 1;
            }
            int off = end == toread ? 1:0;
            int len = end - start - off;
            byte[] tmp = Arrays.copyOfRange(buffer, start, start+len);
            String word = new String(tmp, StandardCharsets.UTF_8);
            long value = 1;
            if (map.containsKey(word)) {
              value = map.get(word) + 1;
            }
            map.put(word, value);
            start = end;
          }
        }
        i += start;
      }
      long endTime = System.currentTimeMillis();
      double diff = (endTime - startTime)*1000;
      // for (String iter : map.keySet()) {
      //   System.out.println(iter + " " + map.get(iter));
      // }
      System.out.println(diff);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      // Closing streams
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
