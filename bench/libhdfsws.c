#include "hdfs.h"
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  long count = 0;
  long start = 0;
  long buflen = 2147483645; // Integer.MAX_VALUE - 2
  struct timeval t1;
  char path[35] = "hdfs://127.0.0.1:9000";
  strcat(path, argv[1]);
  gettimeofday(&t1, NULL);
  long t1s = t1.tv_sec * 1000000L + t1.tv_usec;
  hdfsFS x2 = hdfsConnect("127.0.0.1", 9000);
  hdfsFile x3 = hdfsOpenFile(x2, path, 0, 0, 0, 0);
  hdfsFileInfo *info;
  info = hdfsGetPathInfo(x2, path);
  tOffset length = info->mSize;

  char *buffer = calloc(buflen, sizeof(char));
  long i = 0;
  while (i < length) {
    long diff = length - i;
    int toread = ((long)buflen) > diff ? (int)diff : buflen;
    hdfsPread(x2, x3, i, buffer, toread);
    for (int j = 0; j<toread; j++) {
      if (buffer[j] != ' ') {
        count++;
      }
    }
    i += toread;
  }
  struct timeval t2;
  gettimeofday(&t2, NULL);
  long t2s = t2.tv_sec * 1000000L + t2.tv_usec;
  printf("%ld\n", t2s - t1s);
  free(buffer);
  hdfsFreeFileInfo(info, 1);
  hdfsCloseFile(x2, x3);
  return 1;
}
