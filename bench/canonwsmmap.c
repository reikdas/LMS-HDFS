#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include "scanner_header.h"
#include <sys/time.h>

int main(int argc, char *argv[]) {
    struct timeval t1;
    gettimeofday(&t1, NULL);
    int fd = open(argv[1], 0);
    long size = fsize(fd);
    char *buf = mmap(0, size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_PRIVATE, fd, 0);
    long count = 0;
    for(long i=0; i<size; i++) {
      if (buf[i] == ' ') {
        count++;
      }
    }
    close(fd);
  struct timeval t2;
  gettimeofday(&t2, NULL);
  long t2s = t2.tv_sec * 1000000L + t2.tv_usec;
  printf("%ld\n", t2s - t1s);
    // printf("%ld\n", count);
    return 0;
}

