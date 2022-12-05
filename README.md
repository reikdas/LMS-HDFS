# Parallelization documentation for wordcount

``````
char m1[] = "foo\0ba\0baz\0foo\0ba\0";
char m2[] = "ba\0qu\0";
char m3[] = "qu\0qu\0";

hash =
foo or ba -> 2
qu -> 1
baz -> 0


buf[n][len(txt)]
c_chars_buf[n] = {0}

for s in txt:
which_reducer = hash(s)
buf[which_reducer].append(s)
c_chars_buf[which_reducer] += 1 // (+ char)


M[n][n] = {0}
all_gather(c_chars_buf, M)
M[i][j] = # of chars sent by mapper i to reducer j


# Reducer side

recv_buf[sum(j, M[i][j])] = {0}

for j in 0..n:
tmp = null
...
if my_rank == j:
tmp = recv_buf

    all_gather(buf[j], tmp, j)


counting_operation with hashmap
```
