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
    char* buf = (char*)calloc(size, sizeof(char));
    long sread = read(fd, buf, size);
    long count = 0;
    for(long i=0; i<sread; i++) {
      if (buf[i] == ' ') {
        count++;
      }
    }
    t = clock() - t;
    double time_taken = ((double)t)/CLOCKS_PER_SEC;
    printf("%f\n", time_taken);
    return 0;
}
