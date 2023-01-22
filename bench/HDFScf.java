import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;

public class HDFScf {

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
      int i = 0;
      long[] cf = new long[26];
      for (long c=0; c<length; c+=i) {
        long diff = length - c;
        int toread = ((long)buflen) > diff? (int)diff : buflen;
        in.readFully(c, buffer, 0, toread);
        for (i=0; i<toread; i++) {
          int alphabet = (int)buffer[i];
          if (alphabet >= 65 && alphabet <= 90) {
            cf[alphabet-65] += 1;
          } else if (alphabet >= 97 && alphabet <= 122) {
            cf[alphabet-97] += 1;
          }
        }
      }
      long endTime = System.currentTimeMillis();
      double diff = (endTime - startTime);
      System.out.println(diff);
      // for (i=0; i<26; i++) {
      //   System.out.println((char)(i+65) + " " + cf[i]);
      // }
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
