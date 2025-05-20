package Master;

import Common.Message;
import Common.Metadata;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Master服务接口，定义Master提供的服务
 */
public interface MasterService extends Remote {
    
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
     * 获取表所在的RegionServer
     * @param tableName 表名
     * @return RegionServer列表
     */
    List<String> getTableRegions(String tableName) throws RemoteException;
    
    /**
     * 注册RegionServer
     * @param hostname 主机名
     * @param port 端口号
     * @return 操作结果消息
     */
    Message registerRegionServer(String hostname, int port) throws RemoteException;
    
    /**
     * 获取所有RegionServer列表
     * @return RegionServer列表
     */
    List<String> getAllRegionServers() throws RemoteException;

    /**
     * 获取RegionServer状态
     * @return RegionServer状态
     */
    Boolean getRegionServerStatus(String server) throws RemoteException;
    
    /**
     * 获取所有表信息
     * @return 表信息列表
     */
    List<Metadata.TableInfo> getAllTables() throws RemoteException;

    /**
     * 获取表信息
     */
    Metadata.TableInfo getTableInfo(String tableName) throws RemoteException;

    /**
     * 获取表备份的RegionServer信息
     */
    String getBackupServer(String tableName) throws RemoteException;

    /**
     * 修改表备份的RegionServer信息
     */
    Boolean setBackupServer(String tableName,String backupServer) throws RemoteException;

} 