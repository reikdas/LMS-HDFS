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
    long size = fsize(fd);
    char *buf = mmap(0, size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_PRIVATE, fd, 0);
    long start = 0;
    ht* z = ht_create();
    char *tmp = (char*)malloc(size * sizeof(char));
    while (start < size) {
        while (start < size && isspace(buf[start])) start = start + 1;
        if (start < size) {
            long end = start + 1;
            while (end < size && !isspace(buf[end])) end = end + 1;
            int off = end == size ? 1 : 0;
            int len = end - start - off;
            strncpy(tmp, buf+start, len);
            tmp[len] = '\0';
            long value = ht_get(z, tmp) == -1 ? 1 : ht_get(z, tmp) + 1;
            ht_set(z, tmp, value);
            start = end;
        }
    }
    close(fd);
    t = clock() - t;
    double time_taken = ((double)t)/CLOCKS_PER_SEC;
    printf("%0.3f\n", time_taken);
    free(tmp);
    hti x63 = ht_iterator(z);
    while (ht_next(&x63))
        printf("%s %ld\n", hti_key(&x63), hti_value(&x63));
    return 0;
}
