# 分布式 MiniSQL 项目

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

### 容错机制

系统具有完善的容错和故障恢复能力：

1. **故障检测**：Master 定期检查 RegionServer 状态，通过 ZooKeeper 临时节点监控
2. **优先备份恢复**：故障恢复时优先从备份服务器恢复数据，确保数据完整性
3. **自动重新分片**：当检测到 RegionServer 故障时，会自动将其负责的数据迁移到健康的节点
4. **负载均衡恢复**：选择负载较低的 RegionServer 接管故障节点的数据

## 支持的 SQL 命令

系统支持以下 SQL 命令：

1. **DDL 操作**：

   - `CREATE TABLE` - 创建表
   - `DROP TABLE` - 删除表
   - `CREATE INDEX` - 创建索引
   - `DROP INDEX` - 删除索引

2. **DML 操作**：

   - `INSERT INTO` - 插入数据
   - `DELETE FROM` - 删除数据
   - `UPDATE SET` - 更新数据
   - `SELECT` - 查询数据
   - `SELECT FROM SHARD <server>` - 查询指定分片上的数据

3. **系统命令**：
   - `SHOW SERVERS` - 显示所有 RegionServer 状态
   - `SHOW SHARDS <table>` - 显示表的分片情况
   - `DESCRIBE <table>` - 显示表结构

## 数据存储机制

系统使用文件系统进行数据存储：

1. **序列化**：使用 Java 序列化机制将数据对象转换为字节流存储
2. **文件组织**：每个表在对应的 RegionServer 数据目录下创建独立目录
3. **数据文件**：表数据存储在 `data.db` 文件中
4. **索引文件**：每个索引存储在 `indexes/<index_name>.idx` 文件中
5. **元数据**：表结构元数据存储在 `metadata.dat` 文件中

## 安装和运行

### 环境要求

- JDK 8+
- Maven 3.x
- ZooKeeper 3.6.x

### 编译项目

```
cd Distributed-minisql
mvn clean package
```

### 启动服务

1. **先启动 ZooKeeper 服务**

2. **启动 Master 服务**

   ```
   .\start-master.bat  # Windows
   ./start-master.sh   # Linux/Mac
   ```

3. **启动多个 RegionServer 服务**

   ```
   .\9000-start-region.bat  # 端口 9000
   .\9001-start-region.bat  # 端口 9001
   .\9002-start-region.bat  # 端口 9002
   ```

4. **启动客户端**
   ```
   .\start-client.bat  # Windows
   ./start-client.sh   # Linux/Mac
   ```

## 测试步骤

按照以下步骤测试系统的基本功能：

### 1. 测试表操作和数据分片

1. **创建测试表**

   ```sql
   CREATE TABLE test_table (id INT, name CHAR(100), score FLOAT, PRIMARY KEY(id));
   ```

2. **插入测试数据**

   ```sql
   INSERT INTO test_table (id, name, score) VALUES (1, 'Alice', 95.5);
   INSERT INTO test_table (id, name, score) VALUES (2, 'Bob', 87.0);
   ```

3. **查看分片情况**

   ```sql
   SHOW SHARDS test_table;
   ```

4. **查询所有数据**

   ```sql
   SELECT * FROM test_table;
   ```

5. **查询指定分片数据**
   ```sql
   SELECT FROM SHARD <region_server> * FROM test_table;
   ```

### 2. 测试故障恢复功能

1. **创建测试表**

   ```sql
   CREATE TABLE fault_test (id INT, info CHAR(100), PRIMARY KEY(id));
   ```

2. **插入测试数据**

   ```sql
   INSERT INTO fault_test (id, info) VALUES (1, 'test_recovery_1');
   INSERT INTO fault_test (id, info) VALUES (2, 'test_recovery_2');
   ```

3. **查看表分片情况**

   ```sql
   SHOW SHARDS fault_test;
   ```

4. **停止一个 RegionServer**
   关闭其中一个 RegionServer 进程。

5. **查询数据，验证故障恢复**

   ```sql
   SELECT * FROM fault_test;
   ```

6. **再次查看表分片情况，验证分片调整**
   ```sql
   SHOW SHARDS fault_test;
   ```

## 注意事项

1. 启动至少 3 个 RegionServer 以充分测试容错和备份功能
2. 在分片环境下，数据按主键哈希分布到不同 RegionServer
3. 备份服务器（按名称排序后的最后一个）保存所有表的完整数据备份
4. 若未指定主键，系统将无法确定数据分片位置
