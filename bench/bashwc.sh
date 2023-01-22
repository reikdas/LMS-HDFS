#!/bin/bash
hdfs dfs -cat $1 | grep -Eio "\w+" | sort | uniq -c | awk -F ' ' 'BEGIN { OFS=FS } { print $2, $1 }' > /dev/null 2>&1
