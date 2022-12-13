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
    char* buf = (char*)malloc(size * sizeof(char));
    long sread = read(fd, buf, size);
    long count = 0;
    for(long i=0; i<sread; i++) {
      if (buf[i] == ' ') {
        count++;
      }
    }
    free(buf);
    close(fd);
    t = clock() - t;
    double time_taken = ((double)t)/CLOCKS_PER_SEC;
    printf("%0.3f\n", time_taken);
    printf("%ld\n", count);
    return 0;
}
