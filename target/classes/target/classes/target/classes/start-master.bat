@echo off
REM 启动Master服务
java -cp target\distributed-minisql-1.0-SNAPSHOT-jar-with-dependencies.jar Master.MasterMain
pause 