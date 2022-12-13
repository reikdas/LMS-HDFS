#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include "ht.h"
#include "scanner_header.h"
#include <time.h>

int main(int argc, char *argv[]) {
    clock_t t;
    t = clock();
    int fd = open(argv[1], 0);
    int size = fsize(fd);
    char* buf = (char*)calloc(size, sizeof(char));
    int sread = read(fd, buf, size);
    int start = 0;
    ht* z = ht_create();
    char *tmp = (char*)malloc(size * sizeof(char));
    while (start < sread) {
        while (start < sread && isspace(buf[start])) start = start + 1;
        if (start < sread) {
            int end = start + 1;
            while (end < sread && !isspace(buf[end])) end = end + 1;
            int off = end == sread ? 1 : 0;
            int len = end - start - off;
            strncpy(tmp, buf+start, len);
            tmp[len] = '\0';
            int value = ht_get(z, tmp) == -1 ? 1 : ht_get(z, tmp) + 1;
            ht_set(z, tmp, value);
            start = end;
        }
    }
        t = clock() - t;
    double time_taken = ((double)t)/CLOCKS_PER_SEC;
    printf("%f\n", time_taken);
    return 0;
}

