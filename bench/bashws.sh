#!/bin/bash
hdfs dfs -cat $1 | tr -d '[:blank:] ' | wc -m > /dev/null 2>&1
