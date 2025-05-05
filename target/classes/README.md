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

## 功能测试指南

### 基础功能测试

1. **启动系统**

   ```bash
   # 启动ZooKeeper（确保已安装）
   zkServer.cmd start

   # 启动Master
   start-master.bat

   # 启动多个RegionServer（不同端口）
   start-region.bat 9000（或者java -cp target\distributed-minisql-1.0-SNAPSHOT-jar-with-dependencies.jar RegionServer.RegionMain 9000）
   start-region.bat 9001（或者java -cp target\distributed-minisql-1.0-SNAPSHOT-jar-with-dependencies.jar RegionServer.RegionMain 9001）

   # 启动客户端
   start-client.bat（或者java -cp target\distributed-minisql-1.0-SNAPSHOT-jar-with-dependencies.jar Client.ClientMain）
   ```

2. **表操作测试**

   ```sql
   -- 创建表
   CREATE TABLE test_table (id int, name char(20), score float, PRIMARY KEY(id));

   -- 查看所有表
   tables

   -- 删除表
   DROP TABLE test_table;
   ```

3. **索引操作测试**

   ```sql
   -- 创建索引
   CREATE INDEX idx_name ON test_table (name);

   -- 删除索引
   DROP INDEX idx_name ON test_table;
   ```

4. **数据操作测试**

   ```sql
   -- 插入数据
   INSERT INTO test_table (id, name, score) VALUES (1, 'Alice', 95.5);
   INSERT INTO test_table (id, name, score) VALUES (2, 'Bob', 87.0);

   -- 查询数据
   SELECT * FROM test_table;
   SELECT name, score FROM test_table WHERE id = 1;

   -- 更新数据
   UPDATE test_table SET score = 97.0 WHERE id = 1;

   -- 删除数据
   DELETE FROM test_table WHERE id = 2;
   ```

### 高级功能测试

1. **基本数据操作测试**

首先创建一个测试表并执行基本操作：

```sql
-- 创建测试表
CREATE TABLE basic_test (id INT,name CHAR(50),score FLOAT,PRIMARY KEY(id));

-- 创建索引
CREATE INDEX idx_name ON basic_test (name);

-- 插入数据
INSERT INTO basic_test (id, name, score) VALUES (1, 'Alice', 85.5);
INSERT INTO basic_test (id, name, score) VALUES (2, 'Bob', 92.0);
INSERT INTO basic_test (id, name, score) VALUES (3, 'Charlie', 78.5);

-- 查询数据
SELECT * FROM basic_test;
SELECT name, score FROM basic_test WHERE id > 1;

-- 更新数据
UPDATE basic_test SET score = 88.0 WHERE id = 1;

-- 删除数据
DELETE FROM basic_test WHERE id = 3;

-- 验证操作结果
SELECT * FROM basic_test;
```

2. **分片测试**

通过大量数据测试系统的分片功能（需要启动多个 RegionServer）：

```sql
-- 创建用于分片测试的表
CREATE TABLE shard_test (id INT,data CHAR(200),PRIMARY KEY(id));
```

将以下代码保存为 `insert_data.sql` 文件：

```sql
-- 插入大量数据用于测试分片
INSERT INTO shard_test (id, data) VALUES (1, 'test_data_1');
INSERT INTO shard_test (id, data) VALUES (2, 'test_data_2');
INSERT INTO shard_test (id, data) VALUES (3, 'test_data_3');
-- 继续插入更多数据...
INSERT INTO shard_test (id, data) VALUES (100, 'test_data_100');
```

使用 source 命令执行批量插入：

```
source insert_data.sql
```

验证数据分布（注意观察多个 RegionServer 的日志）：

```sql
-- 查询总数据量
SELECT COUNT(*) FROM shard_test;
SELECT * FROM shard_test;

-- 范围查询验证分片
SELECT * FROM shard_test WHERE id BETWEEN 1 AND 20;
SELECT * FROM shard_test WHERE id BETWEEN 21 AND 40;
```

1. **故障恢复测试**

测试 RegionServer 故障恢复机制：

1. 确保至少启动了两个 RegionServer（例如端口 9000 和 9001）
2. 创建测试表并插入数据

```sql
CREATE TABLE fault_test (id INT,info CHAR(100),PRIMARY KEY(id));

INSERT INTO fault_test (id, info) VALUES (1, 'test_recovery_1');
INSERT INTO fault_test (id, info) VALUES (2, 'test_recovery_2');
INSERT INTO fault_test (id, info) VALUES (3, 'test_recovery_3');
```

3. 查询现有 RegionServer

```
servers
```

4. 停止其中一个 RegionServer（关闭进程）
5. 等待几秒钟，允许 Master 检测到故障
6. 再次查询数据，验证是否仍能访问

```sql
SELECT * FROM fault_test;
```

4. **性能测试**

将以下代码保存为 `performance_test.sql` 文件：

```sql
-- 创建测试表
CREATE TABLE perf_test (
    id INT,
    data CHAR(500),
    PRIMARY KEY(id)
);

-- 插入测试数据（大量数据）
INSERT INTO perf_test (id, data) VALUES (1, REPEAT('a', 500));
-- 循环插入更多记录
-- 注意：这里简化了SQL插入，实际测试中可以使用脚本生成更多数据
INSERT INTO perf_test (id, data) VALUES (2, REPEAT('b', 500));
INSERT INTO perf_test (id, data) VALUES (3, REPEAT('c', 500));
-- ...以此类推插入更多数据

-- 测试查询性能
SELECT COUNT(*) FROM perf_test;
SELECT * FROM perf_test WHERE id BETWEEN 1 AND 50;
```

运行性能测试脚本：

```
source performance_test.sql
```

5. **复杂查询测试**

将以下代码保存为 `complex_queries.sql` 文件：

```sql
-- 创建测试表
CREATE TABLE users (
    user_id INT,
    username CHAR(50),
    age INT,
    PRIMARY KEY(user_id)
);

CREATE TABLE orders (
    order_id INT,
    user_id INT,
    amount FLOAT,
    PRIMARY KEY(order_id)
);

-- 插入测试数据
INSERT INTO users (user_id, username, age) VALUES (1, 'user1', 25);
INSERT INTO users (user_id, username, age) VALUES (2, 'user2', 30);
INSERT INTO users (user_id, username, age) VALUES (3, 'user3', 35);

INSERT INTO orders (order_id, user_id, amount) VALUES (101, 1, 199.99);
INSERT INTO orders (order_id, user_id, amount) VALUES (102, 1, 99.50);
INSERT INTO orders (order_id, user_id, amount) VALUES (103, 2, 149.99);

-- 复杂查询测试
-- 注意：根据系统支持的SQL特性可能需要调整查询
SELECT u.username, COUNT(o.order_id) AS order_count
FROM users u, orders o
WHERE u.user_id = o.user_id
GROUP BY u.username;

-- 聚合函数测试
SELECT MAX(amount), MIN(amount), AVG(amount) FROM orders;

-- 条件查询
SELECT * FROM orders WHERE amount > 100;
```

运行复杂查询测试：

```
source complex_queries.sql
```

## 系统监控

在测试过程中，可以通过以下命令监控系统状态：

```
-- 查看所有RegionServer
servers

-- 查看所有表
tables
```

注意观察 Master 和 RegionServer 的日志输出，以验证系统行为是否符合预期。

## 系统管理命令

可使用以下命令进行系统管理：

### 表结构查看

```
DESCRIBE tablename;   # 或使用简写形式: DESC tablename;
```

显示表的结构，包括列名、数据类型、约束和索引信息。

### 分片信息查看

```
SHOW SHARDS tablename;
```

显示表的分片情况，包括所有存储该表的 RegionServer 节点。

### 服务器状态查看

```
SHOW SERVERS;
```

显示所有 RegionServer 的状态，包括运行时间和表数量等信息。

## 批量数据操作

### 批量 SQL 执行

```
SOURCE sqlfile.sql;
```

从指定的 SQL 文件中批量执行 SQL 语句。SQL 文件中的每条语句应以分号结尾。

### 数据导入

```
IMPORT CSV 'filename.csv' INTO tablename;
IMPORT JSON 'filename.json' INTO tablename;
```

支持从 CSV 或 JSON 文件导入数据到指定表中。

- CSV 文件格式：第一行为列名，后续每行为数据记录
- JSON 文件格式：包含对象数组，每个对象代表一条记录

## 示例文件

系统在 samples 目录中提供了示例数据文件供参考：

- `samples/sample.csv`：CSV 格式的示例数据
- `samples/sample.json`：JSON 格式的示例数据
- `samples/sample.sql`：批量 SQL 执行的示例文件
