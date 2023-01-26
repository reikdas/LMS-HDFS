import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;

public class HDFSws {

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
      for (long c=0; c<length; c+=i) {
        long diff = length - c;
        int toread = ((long)buflen) > diff? (int)diff : buflen;
        in.readFully(c, buffer, 0, toread);
        for (i=0; i<toread; i++) {
          if (buffer[i] != 32) { count++; }
        }
      }
      long endTime = System.currentTimeMillis();
      // System.out.println("Count = " + count);
      double diff = (endTime - startTime)*1000;
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
