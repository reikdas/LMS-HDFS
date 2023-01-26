#include "hdfs.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  long count = 0;
  long start = 0;
  long limit = 1000000;
  struct timeval t1;
  char path[35] = "hdfs://127.0.0.1:9000";
  strcat(path, argv[1]);
  gettimeofday(&t1, NULL);
  long t1s = t1.tv_sec * 1000000L + t1.tv_usec;
  hdfsFS x2 = hdfsConnect("127.0.0.1", 9000);
  hdfsFile x3 = hdfsOpenFile(x2, path, 0, 0, 0, 0);
  hdfsFileInfo *info;
  info = hdfsGetPathInfo(x2, path);
  tOffset x4 = info->mSize;
  long remain = x4 % limit;
  long each = x4 / limit;
  char *x5 = calloc(limit + 1, sizeof(char));
  long cf[26];
  for (int i = 1; i <= x4 / limit; i++) {
    hdfsPread(x2, x3, start, x5, limit);
    for (int j = 0; j < limit; j++) {
      int c = (int)x5[j];
      if (c >= 65 && c <= 90) {
        cf[c - 65] += 1;
      } else if (c >= 97 && c <= 122) {
        cf[c - 97] += 1;
      }
    }
    start = (i * limit) + 1;
  }
  hdfsPread(x2, x3, start, x5, remain);
  for (int j = 0; j < remain; j++) {
    int c = (int)x5[j];
    if (c >= 65 && c <= 90) {
      cf[c - 65] += 1;
    } else if (c >= 97 && c <= 122) {
      cf[c - 97] += 1;
    }
  }
  struct timeval t2;
  gettimeofday(&t2, NULL);
  long t2s = t2.tv_sec * 1000000L + t2.tv_usec;
  printf("%ld\n", t2s - t1s);
  for (int i=0; i<26; i++) {
    printf("%ld\n", cf[i]);
  }
  free(x5);
  hdfsFreeFileInfo(info, 1);
  hdfsCloseFile(x2, x3);
  return 0;
}
