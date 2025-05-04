#!/bin/bash

# 默认端口
PORT=9000

# 如果提供了端口参数，则使用提供的端口
if [ "$1" != "" ]; then
    PORT=$1
fi

# 启动RegionServer服务
java -cp target/distributed-minisql-1.0-SNAPSHOT-jar-with-dependencies.jar RegionServer.RegionMain $PORT 