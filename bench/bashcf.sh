#!/bin/bash
hdfs dfs -cat $1 | awk -vFS="" '{for(i=1;i<=NF;i++)w[toupper($i)]++}END{for(i in w) if (match(i,/[A-Z]/)) {print i,w[i]}}' > /dev/null 2>&1
