#!/bin/bash
# Get 1G.txt from /scratch1/das160 (bigdata2)
touch 10G.txt
for (( i = 0; i < 10; i++ )); do
    cat 1G.txt >> 10G.txt
done
# Download enwiki9 + filter from http://mattmahoney.net/dc/textdata.html
hdfs dfs -moveFromLocal 1G.txt /1G.txt
hdfs dfs -moveFromLocal 10G.txt /10G.txt