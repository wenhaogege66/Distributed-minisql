import random
import string

# 生成随机字符串函数
def random_string(length=100):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# 生成 SQL 插入语句并写入文件
num_records = 100  # 设置需要生成的数据量
file_name = "insert_data100.sql"

with open(file_name, "w") as file:
    for i in range(num_records):
        info = random_string()
        sql_statement = f"INSERT INTO fault_test (id, info) VALUES ({i}, '{info}');\n"
        file.write(sql_statement)

print(f"{num_records} 条 INSERT 语句已成功写入 {file_name} 文件！")