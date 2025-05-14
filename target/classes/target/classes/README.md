# 分布式 MiniSQL 项目

## 项目结构

- Master：主节点，负责管理 RegionServer 和数据分布
- RegionServer：区域服务器，管理实际数据存储和查询
- Client：客户端，提供用户接口
- Common：公共组件，包含共享的数据结构和工具类

## 系统架构

本项目是一个分布式数据库系统，使用 Zookeeper 进行协调，实现数据的分布式存储和管理。系统具有高可用性、可扩展性以及容错能力。

### 核心组件

1. **Master**：

   - 管理元数据（表结构、索引等）
   - 监控和管理 RegionServer
   - 协调数据分布和负载均衡
   - 处理表创建、删除等 DDL 操作
   - 实现故障检测和恢复

2. **RegionServer**：

   - 存储和管理实际数据
   - 处理数据查询、插入、更新和删除等 DML 操作
   - 管理索引和数据文件
   - 支持数据复制和故障恢复

3. **ZooKeeper**：

   - 协调 Master 和 RegionServer
   - 提供服务发现和注册功能
   - 管理系统状态和配置信息

4. **Client**：
   - 提供用户接口
   - 解析 SQL 语句
   - 与 Master 和 RegionServer 通信

## 系统特性

### 数据分片策略

系统采用哈希分片策略将数据分布在多个 RegionServer 上：

1. **表分片**：创建表时，系统会为表分配多个 RegionServer 节点（默认为 2 个副本）
2. **数据分片**：数据按主键哈希分配到不同 RegionServer
3. **专用备份服务器**：当存在 3 个以上的 RegionServer 时，系统会指定一个 RegionServer 专门用于备份（按名称排序后的最后一个）
4. **备份复制策略**：备份服务器保存所有表的完整数据，而其他服务器只保存分片数据

### 容错机制

系统具有完善的容错和故障恢复能力：

1. **故障检测**：Master 定期检查 RegionServer 状态，通过 ZooKeeper 临时节点和心跳机制监控
2. **智能数据恢复**：
   - 故障恢复时优先从备份服务器恢复数据，确保数据完整性
   - 若非备份服务器故障，则根据分片规则只恢复应分配给新节点的数据
   - 将每个表的数据按照主键哈希值重新分配，避免全量数据冗余复制
3. **资源利用优化**：
   - 只有当副本数不足时才添加新节点接管数据
   - 选择负载较低的 RegionServer 接管故障节点的数据分片
4. **多副本容错**：每个表默认维护 2 个副本，可以容忍单节点故障

## 系统设计

### 通信模型

系统使用 Java RMI 进行组件间通信，客户端首先连接到 Master 获取表的元数据和存储位置信息，然后根据需要直接与 RegionServer 通信进行数据操作。

### 数据分布

系统采用水平分片方式将表数据分布在多个 RegionServer 上，根据表的主键或指定的分片键进行数据分布。Master 负责维护表的分片信息，并在查询时将请求路由到正确的 RegionServer。

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

1. 首先确保 ZooKeeper 服务已启动

   清理 ZooKeeper 中的数据：

   ```
   zkCli.cmd
   deleteall /tables
   deleteall /master
   deleteall /region-servers
   ```

2. 启动 Master 服务

   ```bash
   # Windows
   start-master.bat
   ```

3. 启动 RegionServer 服务（建议启动至少 3 个，以支持容错功能）

   ```bash
   # Windows
   start-region.bat 9000
   start-region.bat 9001
   start-region.bat 9002
   ```

4. 启动客户端

   ```bash
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

1. **表操作测试**

   ```sql
   -- 创建表
   CREATE TABLE test_table (id int, name char(20), score float, PRIMARY KEY(id));

   -- 查看所有表
   tables

   -- 删除表
   DROP TABLE test_table;
   ```

2. **索引操作测试**

   ```sql
   -- 创建索引
   CREATE INDEX idx_name ON test_table (name);

   -- 删除索引
   DROP INDEX idx_name ON test_table;
   ```

3. **数据操作测试**

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

### 分片测试

1. **创建分片测试表并查看分片情况**

   ```sql
   -- 创建测试表
   CREATE TABLE shard_test (id INT, data CHAR(100), PRIMARY KEY(id));

   -- 查看分片情况
   SHOW SHARDS shard_test;

   -- 查看所有RegionServer
   SHOW SERVERS;
   ```

2. **插入数据测试分片**

   ```sql
   -- 插入数据（根据id的哈希值分配到不同RegionServer）
   INSERT INTO shard_test (id, data) VALUES (1, 'data-1');
   INSERT INTO shard_test (id, data) VALUES (2, 'data-2');
   -- 继续插入多条数据...
   INSERT INTO shard_test (id, data) VALUES (100, 'data-100');

   -- 查询特定分片的数据（语法：SELECT FROM SHARD <server:port> <columns> FROM <table> [WHERE <conditions>]）
   -- 假设DESKTOP-XXX:9000是其中一个分片
   SELECT FROM SHARD DESKTOP-RJS0D8P:9001 * FROM shard_test;
   ```

### 容错测试

1. **准备测试环境**

   确保启动了至少 3 个 RegionServer（例如端口 9000、9001 和 9002）

2. **创建测试表并插入数据**

   ```sql
   CREATE TABLE fault_test (id INT, info CHAR(100), PRIMARY KEY(id));

   INSERT INTO fault_test (id, info) VALUES (1, 'test_recovery_1');
   INSERT INTO fault_test (id, info) VALUES (2, 'test_recovery_2');
   INSERT INTO fault_test (id, info) VALUES (3, 'test_recovery_3');

   -- 查看表的分片情况
   SHOW SHARDS fault_test;
   ```

3. **模拟 RegionServer 故障**

   - 记录当前负责 fault_test 表的 RegionServer
   - 关闭其中一个 RegionServer（直接关闭命令窗口）
   - 等待几秒钟，允许 Master 检测到故障并进行恢复
   - 再次查询数据，验证数据是否仍然可访问

   ```sql
   -- 查询数据，确认系统仍能正常工作
   SELECT * FROM fault_test;

   -- 查看表的分片情况，应该已经自动更新
   SHOW SHARDS fault_test;
   ```

## SQL 命令参考

### 数据定义语言 (DDL)

```sql
-- 创建表
CREATE TABLE table_name (
    column1 INT,
    column2 CHAR(50),
    column3 FLOAT,
    PRIMARY KEY(column1)
);

-- 删除表
DROP TABLE table_name;

-- 创建索引
CREATE INDEX index_name ON table_name (column_name);

-- 删除索引
DROP INDEX index_name ON table_name;
```

### 数据操作语言 (DML)

```sql
-- 插入数据
INSERT INTO table_name (column1, column2) VALUES (value1, value2);

-- 查询数据
SELECT column1, column2 FROM table_name WHERE column1 = value;

-- 范围查询
SELECT * FROM table_name WHERE column BETWEEN min_value AND max_value;

-- 更新数据
UPDATE table_name SET column1 = new_value WHERE column2 = condition_value;

-- 删除数据
DELETE FROM table_name WHERE column = value;
```

### 系统管理命令

```sql
-- 查看所有表
tables

-- 查看所有RegionServer
SHOW SERVERS;

-- 查看表结构
DESCRIBE table_name;
DESC table_name;

-- 查看表的分片情况
SHOW SHARDS table_name;

-- 从特定分片查询数据
SELECT FROM SHARD server:port column1, column2 FROM table_name WHERE condition;

-- 批量执行SQL文件
SOURCE file_path.sql;

-- 导入数据
IMPORT CSV 'filename.csv' INTO table_name;
IMPORT JSON 'filename.json' INTO table_name;
```

## 注意事项

1. RegionServer 启动时会自动连接到 Master 注册，Master 负责表的创建和数据分片
2. 确保 ZooKeeper 服务正常运行，系统依赖它进行协调和服务发现
3. 优先使用 3 个以上的 RegionServer 以充分利用容错机制
4. 每个 RegionServer 应使用独立的工作目录，以模拟真实的分布式环境
