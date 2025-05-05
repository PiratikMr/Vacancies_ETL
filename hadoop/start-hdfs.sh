#!/bin/bash
mkdir -p /opt/hadoop/data/nameNode

if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force
fi

hdfs namenode