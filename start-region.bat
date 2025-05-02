@echo off
REM 默认端口
set PORT=9000

REM 如果提供了端口参数，则使用提供的端口
if not "%1"=="" (
    set PORT=%1
)

REM 启动RegionServer服务
java -cp target\distributed-minisql-1.0-SNAPSHOT-jar-with-dependencies.jar RegionServer.RegionMain %PORT%
pause 