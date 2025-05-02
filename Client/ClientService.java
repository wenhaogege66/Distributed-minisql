package Client;

import Common.Message;
import Common.Metadata;

import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * 客户端服务接口
 */
public interface ClientService {
    
    /**
     * 连接到系统
     * @param masterHost Master主机地址
     * @param masterPort Master端口
     * @return 连接结果
     */
    boolean connect(String masterHost, int masterPort);
    
    /**
     * 断开与系统的连接
     */
    void disconnect();
    
    /**
     * 执行SQL语句
     * @param sql SQL语句
     * @return 执行结果
     */
    Message executeSql(String sql);
    
    /**
     * 创建表
     * @param tableInfo 表信息
     * @return 操作结果
     */
    Message createTable(Metadata.TableInfo tableInfo);
    
    /**
     * 删除表
     * @param tableName 表名
     * @return 操作结果
     */
    Message dropTable(String tableName);
    
    /**
     * 创建索引
     * @param indexInfo 索引信息
     * @return 操作结果
     */
    Message createIndex(Metadata.IndexInfo indexInfo);
    
    /**
     * 删除索引
     * @param indexName 索引名
     * @param tableName 表名
     * @return 操作结果
     */
    Message dropIndex(String indexName, String tableName);
    
    /**
     * 插入数据
     * @param tableName 表名
     * @param values 列值映射
     * @return 操作结果
     */
    Message insert(String tableName, Map<String, Object> values);
    
    /**
     * 删除数据
     * @param tableName 表名
     * @param conditions 条件映射
     * @return 操作结果
     */
    Message delete(String tableName, Map<String, Object> conditions);
    
    /**
     * 查询数据
     * @param tableName 表名
     * @param columns 需要查询的列
     * @param conditions 条件映射
     * @return 查询结果
     */
    List<Map<String, Object>> select(String tableName, List<String> columns, Map<String, Object> conditions);
    
    /**
     * 更新数据
     * @param tableName 表名
     * @param values 需要更新的列值映射
     * @param conditions 条件映射
     * @return 操作结果
     */
    Message update(String tableName, Map<String, Object> values, Map<String, Object> conditions);
    
    /**
     * 获取所有表信息
     * @return 表信息列表
     */
    List<Metadata.TableInfo> getAllTables();
    
    /**
     * 获取所有RegionServer信息
     * @return RegionServer列表
     */
    List<String> getAllRegionServers();
} 