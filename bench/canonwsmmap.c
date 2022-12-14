#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include "scanner_header.h"
#include <time.h>

int main(int argc, char *argv[]) {
    clock_t t;
    t = clock();
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
    t = clock() - t;
    double time_taken = ((double)t)/CLOCKS_PER_SEC;
    printf("%0.3f\n", time_taken);
    printf("%ld\n", count);
    return 0;
}

