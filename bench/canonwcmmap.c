#include "ht.h"
#include "scanner_header.h"
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  struct timeval t1;
  gettimeofday(&t1, NULL);
  long t1s = t1.tv_sec * 1000000L + t1.tv_usec;
  int fd = open(argv[1], 0);
  long size = fsize(fd);
  char *buf =
      mmap(0, size, PROT_READ | PROT_WRITE, MAP_FILE | MAP_PRIVATE, fd, 0);
  long start = 0;
  ht *z = ht_create();
  char *tmp = (char *)malloc(size * sizeof(char));
  while (start < size) {
    while (start < size && isspace(buf[start]))
      start = start + 1;
    if (start < size) {
      long end = start + 1;
      while (end < size && !isspace(buf[end]))
        end = end + 1;
      int off = end == size ? 1 : 0;
      int len = end - start - off;
      strncpy(tmp, buf + start, len);
      tmp[len] = '\0';
      long value = ht_get(z, tmp) == -1 ? 1 : ht_get(z, tmp) + 1;
      ht_set(z, tmp, value);
      start = end;
    }
  }
  close(fd);
  struct timeval t2;
  gettimeofday(&t2, NULL);
  long t2s = t2.tv_sec * 1000000L + t2.tv_usec;
  printf("%ld\n", t2s - t1s);
  free(tmp);
  hti x63 = ht_iterator(z);
  while (ht_next(&x63))
      printf("%s %ld\n", hti_key(&x63), hti_value(&x63));
  return 0;
}
