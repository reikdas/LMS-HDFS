#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include "scanner_header.h"
#include <stdlib.h>
#include <sys/time.h>

int main(int argc, char *argv[]) {
    struct timeval t1;
    gettimeofday(&t1, NULL);
    int fd = open(argv[1], 0);
    long size = fsize(fd);
    char *buf = mmap(0, size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_PRIVATE, fd, 0);
    long* arr = calloc(26, sizeof(long));
    for(long i=0; i<size; i++) {
      int c = (int)buf[i];
      if (c>=65 && c <=90) {
        arr[c-65] += 1;
      } else if (c >= 97 && c<=122) {
        arr[c-97] += 1;
      }
    }
    close(fd);
  struct timeval t2;
  gettimeofday(&t2, NULL);
  long t2s = t2.tv_sec * 1000000L + t2.tv_usec;
  printf("%ld\n", t2s - t1s);
    free(arr);
    // for (int i=0; i<26; i++) {
    //   printf("%ld\n", arr[i]);
    // }
    return 0;
}
