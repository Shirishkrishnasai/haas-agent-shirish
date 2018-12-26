#! /bin/bash
# format namenode...need to check this
if [ ! -d /opt/hdfs/namenode/current ]; then
        echo "Formatting namenode"
        /opt/hadoop/bin/hdfs namenode -format
    
fi


