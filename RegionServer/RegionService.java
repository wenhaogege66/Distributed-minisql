package RegionServer;

import Common.Message;
import Common.Metadata;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * RegionServer服务接口
 */
public interface RegionService extends Remote {
    
    /**
     * 创建表
     * @param tableInfo 表信息
     * @return 操作结果消息
     */
    Message createTable(Metadata.TableInfo tableInfo) throws RemoteException;
    
    /**
     * 删除表
     * @param tableName 表名
     * @return 操作结果消息
     */
    Message dropTable(String tableName) throws RemoteException;
    
    /**
     * 创建索引
     * @param indexInfo 索引信息
     * @return 操作结果消息
     */
    Message createIndex(Metadata.IndexInfo indexInfo) throws RemoteException;
    
    /**
     * 删除索引
     * @param indexName 索引名
     * @param tableName 表名
     * @return 操作结果消息
     */
    Message dropIndex(String indexName, String tableName) throws RemoteException;
    
    /**
     * 插入数据
     * @param tableName 表名
     * @param values 列值映射
     * @return 操作结果消息
     */
    Message insert(String tableName, Map<String, Object> values) throws RemoteException;
    
    /**
     * 删除数据
     * @param tableName 表名
     * @param conditions 条件映射
     * @return 操作结果消息
     */
    Message delete(String tableName, Map<String, Object> conditions) throws RemoteException;
    
    /**
     * 查询数据
     * @param tableName 表名
     * @param columns 需要查询的列
     * @param conditions 条件映射
     * @return 查询结果
     */
    List<Map<String, Object>> select(String tableName, List<String> columns, Map<String, Object> conditions) throws RemoteException;
    
    /**
     * 更新数据
     * @param tableName 表名
     * @param values 需要更新的列值映射
     * @param conditions 条件映射
     * @return 操作结果消息
     */
    Message update(String tableName, Map<String, Object> values, Map<String, Object> conditions) throws RemoteException;
    
    /**
     * 获取服务器状态
     * @return 服务器状态信息
     */
    Map<String, Object> getStatus() throws RemoteException;
} 