#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include "scanner_header.h"
#include <stdlib.h>

int main(int argc, char *argv[]) {
    clock_t t;
    t = clock();
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
    free(buf);
    free(arr);
    close(fd);
    t = clock() - t;
    double time_taken = ((double)t)/CLOCKS_PER_SEC;
    printf("%0.3f\n", time_taken);
    for (int i=0; i<26; i++) {
      printf("%ld\n", arr[i]);
    }
    return 0;
}
