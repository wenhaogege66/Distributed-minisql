# 分布式 MiniSQL 项目

## 项目结构

- Master：主节点，负责管理 RegionServer 和数据分布
- RegionServer：区域服务器，管理实际数据存储和查询
- Client：客户端，提供用户接口
- Common：公共组件，包含共享的数据结构和工具类

## 系统架构

本项目是一个分布式数据库系统，使用 Zookeeper 进行协调，实现数据的分布式存储和管理。系统具有高可用性和可扩展性。

### 核心组件

1. **Master**：

   - 管理元数据（表结构、索引等）
   - 监控和管理 RegionServer
   - 协调数据分布和负载均衡
   - 处理表创建、删除等 DDL 操作

2. **RegionServer**：

   - 存储和管理实际数据
   - 处理数据查询、插入、更新和删除等 DML 操作
   - 管理索引和数据文件

3. **ZooKeeper**：

   - 协调 Master 和 RegionServer
   - 提供服务发现和注册功能
   - 管理系统状态和配置信息

4. **Client**：
   - 提供用户接口
   - 解析 SQL 语句
   - 与 Master 和 RegionServer 通信

## 系统设计

### 通信模型

系统使用 Java RMI 进行组件间通信，客户端首先连接到 Master 获取表的元数据和存储位置信息，然后根据需要直接与 RegionServer 通信进行数据操作。

### 数据分布

系统采用水平分片方式将表数据分布在多个 RegionServer 上，根据表的主键或指定的分片键进行数据分布。Master 负责维护表的分片信息，并在查询时将请求路由到正确的 RegionServer。

### 容错机制

系统使用 ZooKeeper 监控服务节点状态，当 RegionServer 故障时，Master 会将其负责的数据区域重新分配给其他可用的 RegionServer。对于 Master 故障，系统支持 Master 节点的自动选举和恢复。

## 运行环境要求

- Java 11+
- ZooKeeper 3.9.3+
- Maven 3.6+

## 构建和运行

### 构建项目

```bash
mvn clean package
```

### 启动服务

1. 首先确保 ZooKeeper 服务已启动（zkServer.cmd）

清理 ZooKeeper 中的数据：
zkCli.cmd
deleteall /tables
deleteall /master
deleteall /region-servers

2. 启动 Master 服务

```bash
# Linux/Mac
./start-master.sh

# Windows
start-master.bat

#打印
Master started on port 8000
```

3. 启动 RegionServer 服务（可以启动多个，端口不同）

```bash
# Linux/Mac
./start-region.sh 9000

# Windows
start-region.bat 9000

#打印
RegionServer registered: DESKTOP-RJS0D8P:9000
RegionServer started on DESKTOP-RJS0D8P:9000
```

4. 启动客户端

```bash
# Linux/Mac
./start-client.sh

# Windows
start-client.bat
```

## 接口设计

系统提供了以下主要接口：

1. **Master 接口**：提供表管理、索引管理和 RegionServer 管理功能
2. **RegionServer 接口**：提供数据操作功能，如查询、插入、更新和删除
3. **客户端接口**：提供 SQL 解析和执行功能

详细的接口定义可以查看各组件的接口文件。

## 测试

help
CREATE TABLE student (id int, name char(20), age int, PRIMARY KEY(id));
tables
CREATE INDEX idx_name ON student (name);
INSERT INTO student (id, name, age) VALUES (1, 'Zhang San', 20);
SELECT \* FROM student;
