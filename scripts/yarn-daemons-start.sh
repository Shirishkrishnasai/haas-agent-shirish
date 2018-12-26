#! /bin/sh
#start hdfs daemons in all  nodes

if [ ! -d /opt/hdfs/namenode/current/in_use.lock ]; then
        echo "starting hdfs daemons"
        /opt/hadoop/sbin/start-dfs.sh
fi

