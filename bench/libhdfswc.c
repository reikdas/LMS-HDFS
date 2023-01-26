#include "hdfs.h"
#include "ht.h"
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
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
  char *tmp = (char *)malloc(length * sizeof(char));
  ht *map = ht_create();
  int flag = 0;

  long i = 0;
  while (i < length) {
    long diff = length - i;
    int toread = ((long)buflen) > diff ? (int)diff : buflen;
    hdfsPread(x2, x3, i, buffer, toread);
    int start = 0;
    while (start < toread) {
      while (start < toread && isspace(buffer[start]))
        start = start + 1;
      if (start < toread) {
        int end = start + 1;
        while (end < toread && !isspace(buffer[end]))
          end = end + 1;
        int off = end == toread ? 1 : 0;
        int len = end - start - off;
        strncpy(tmp, buffer + start, len);
        tmp[len] = '\0';
        long value = ht_get(map, tmp) == -1 ? 1 : ht_get(map, tmp) + 1;
        ht_set(map, tmp, value);
        start = end;
      }
    }
    i += start;
  }

  struct timeval t2;
  gettimeofday(&t2, NULL);
  long t2s = t2.tv_sec * 1000000L + t2.tv_usec;
  free(buffer);
  free(tmp);
  hti iter = ht_iterator(map);
  while (ht_next(&iter)) {
    printf("%s %ld\n", hti_key(&iter), hti_value(&iter));
  }
  printf("%ld\n", t2s - t1s);
  hdfsFreeFileInfo(info, 1);
  hdfsCloseFile(x2, x3);
  return 0;
}
