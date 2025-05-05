-- 创建测试表
CREATE TABLE test_batch (
  id INT NOT NULL,
  name VARCHAR(50) NOT NULL,
  age INT,
  email VARCHAR(100) UNIQUE,
  PRIMARY KEY(id)
);

-- 插入测试数据
INSERT INTO test_batch (id, name, age, email) VALUES (1, 'Zhang San', 25, 'zhangsan@example.com');
INSERT INTO test_batch (id, name, age, email) VALUES (2, 'Li Si', 30, 'lisi@example.com');
INSERT INTO test_batch (id, name, age, email) VALUES (3, 'Wang Wu', 22, 'wangwu@example.com');

-- 创建索引
CREATE INDEX idx_age ON test_batch(age);

-- 测试查询
SELECT * FROM test_batch; 