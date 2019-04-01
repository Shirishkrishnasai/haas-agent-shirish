#!/bin/sh
sudo /opt/hadoop/bin/hdfs dfs -get $1 /opt/mnt/azurefileshare/hdfs/$2

