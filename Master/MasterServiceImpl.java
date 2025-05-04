package Master;

import Common.Message;
import Common.Metadata;
import Common.RPCUtils;
import Common.ZKUtils;
import RegionServer.RegionService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Master服务实现类
 */
public class MasterServiceImpl extends UnicastRemoteObject implements MasterService, Watcher {
    
    private ZKUtils zkUtils;
    
    // 表元数据管理
    private Map<String, Metadata.TableInfo> tables;
    
    // 表区域映射
    private Map<String, Metadata.TableRegionInfo> tableRegions;
    
    // RegionServer管理
    private Map<String, String> regionServers; // key: hostname:port, value: status
    
    /**
     * 构造函数
     */
    public MasterServiceImpl() throws RemoteException {
        super();
        
        tables = new ConcurrentHashMap<>();
        tableRegions = new ConcurrentHashMap<>();
        regionServers = new ConcurrentHashMap<>();
        
        // 初始化ZooKeeper连接
        initZooKeeper();
    }
    
    /**
     * 初始化ZooKeeper连接和节点
     */
    private void initZooKeeper() {
        try {
            zkUtils = new ZKUtils();
            zkUtils.connect();
            zkUtils.initZKNodes();
            
            // 注册Master节点
            registerMasterNode();
            
            // 监听RegionServer节点变化
            watchRegionServers();
            
            // 恢复表元数据
            recoverTableMetadata();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 恢复表元数据
     */
    private void recoverTableMetadata() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        List<String> tableNodes = zkUtils.getChildren(ZKUtils.TABLES_NODE, this);
        for (String tableName : tableNodes) {
            String tablePath = ZKUtils.TABLES_NODE + "/" + tableName;
            byte[] data = zkUtils.getData(tablePath, null);
            if (data != null && data.length > 0) {
                Metadata.TableInfo tableInfo = deserialize(data);
                tables.put(tableName, tableInfo);
                
                // 恢复表区域信息
                String regionPath = tablePath + "/regions";
                if (zkUtils.exists(regionPath, null)) {
                    byte[] regionData = zkUtils.getData(regionPath, null);
                    if (regionData != null && regionData.length > 0) {
                        Metadata.TableRegionInfo regionInfo = deserialize(regionData);
                        tableRegions.put(tableName, regionInfo);
                    }
                }
            }
        }
    }
    
    /**
     * 注册Master节点
     */
    private void registerMasterNode() throws KeeperException, InterruptedException {
        String masterData = "master_active";
        zkUtils.createNode(ZKUtils.MASTER_NODE, masterData.getBytes(), CreateMode.EPHEMERAL);
    }
    
    /**
     * 监听RegionServer节点变化
     */
    private void watchRegionServers() throws KeeperException, InterruptedException {
        List<String> children = zkUtils.getChildren(ZKUtils.REGION_SERVERS_NODE, this);
        
        // 初始化RegionServer列表
        for (String child : children) {
            String path = ZKUtils.REGION_SERVERS_NODE + "/" + child;
            byte[] data = zkUtils.getData(path, this);
            String regionServerInfo = new String(data);
            regionServers.put(child, regionServerInfo);
        }
    }
    
    @Override
    public void process(WatchedEvent event) {
        try {
            // 处理RegionServer节点变化事件
            if (event.getPath() != null && event.getPath().startsWith(ZKUtils.REGION_SERVERS_NODE)) {
                watchRegionServers();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public Message createTable(Metadata.TableInfo tableInfo) throws RemoteException {
        try {
            String tableName = tableInfo.getTableName();
            
            // 检查表是否已存在
            if (tables.containsKey(tableName)) {
                return Message.createErrorResponse("master", "client", "表已存在: " + tableName);
            }
            
            // 保存表元数据
            tables.put(tableName, tableInfo);
            
            // 创建表节点
            String tablePath = ZKUtils.TABLES_NODE + "/" + tableName;
            zkUtils.createNode(tablePath, serialize(tableInfo), CreateMode.PERSISTENT);
            
            // 为表分配RegionServer
            List<String> allRegionServers = new ArrayList<>(regionServers.keySet());
            if (allRegionServers.isEmpty()) {
                return Message.createErrorResponse("master", "client", "没有可用的RegionServer");
            }
            
            // 简单的负载均衡: 选择第一个RegionServer
            String selectedRegionServer = allRegionServers.get(0);
            
            // 记录表区域信息
            Metadata.TableRegionInfo regionInfo = new Metadata.TableRegionInfo(tableName);
            regionInfo.addRegionServer(selectedRegionServer);
            tableRegions.put(tableName, regionInfo);
            
            // 保存表区域信息到ZooKeeper
            String regionPath = tablePath + "/regions";
            zkUtils.createNode(regionPath, serialize(regionInfo), CreateMode.PERSISTENT);
            
            // 通知RegionServer创建表
            RegionService regionService = RPCUtils.getRegionService(selectedRegionServer);
            if (regionService != null) {
                Message response = regionService.createTable(tableInfo);
                if (response.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                    // 如果RegionServer创建失败，回滚Master的操作
                    tables.remove(tableName);
                    tableRegions.remove(tableName);
                    zkUtils.deleteNode(regionPath);
                    zkUtils.deleteNode(tablePath);
                    return Message.createErrorResponse("master", "client", "RegionServer创建表失败: " + response.getData("error"));
                }
            } else {
                return Message.createErrorResponse("master", "client", "无法连接到RegionServer: " + selectedRegionServer);
            }
            
            return Message.createSuccessResponse("master", "client");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("master", "client", "创建表失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message dropTable(String tableName) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tables.containsKey(tableName)) {
                return Message.createErrorResponse("master", "client", "表不存在: " + tableName);
            }
            
            // 获取表所在的RegionServer
            Metadata.TableRegionInfo regionInfo = tableRegions.get(tableName);
            List<String> servers = regionInfo.getRegionServers();
            
            // 通知所有相关RegionServer删除表
            boolean allSuccess = true;
            for (String server : servers) {
                try {
                    RegionService regionService = RPCUtils.getRegionService(server);
                    if (regionService != null) {
                        Message response = regionService.dropTable(tableName);
                        if (response.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                            allSuccess = false;
                        }
                    }
                } catch (Exception e) {
                    allSuccess = false;
                    e.printStackTrace();
                }
            }
            
            if (!allSuccess) {
                return Message.createErrorResponse("master", "client", "部分RegionServer删除表失败");
            }
            
            // 删除表元数据
            tables.remove(tableName);
            tableRegions.remove(tableName);
            
            // 删除表节点
            String tablePath = ZKUtils.TABLES_NODE + "/" + tableName;
            // 首先删除子节点
            String regionsPath = tablePath + "/regions";
            if (zkUtils.exists(regionsPath, null)) {
                zkUtils.deleteNode(regionsPath);
            }
            // 然后删除表节点
            zkUtils.deleteNode(tablePath);
            
            return Message.createSuccessResponse("master", "client");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("master", "client", "删除表失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message createIndex(Metadata.IndexInfo indexInfo) throws RemoteException {
        try {
            String tableName = indexInfo.getTableName();
            String indexName = indexInfo.getIndexName();
            
            // 检查表是否存在
            if (!tables.containsKey(tableName)) {
                return Message.createErrorResponse("master", "client", "表不存在: " + tableName);
            }
            
            Metadata.TableInfo tableInfo = tables.get(tableName);
            
            // 检查索引是否已存在
            for (Metadata.IndexInfo index : tableInfo.getIndexes()) {
                if (index.getIndexName().equals(indexName)) {
                    return Message.createErrorResponse("master", "client", "索引已存在: " + indexName);
                }
            }
            
            // 添加索引
            tableInfo.addIndex(indexInfo);
            
            // 获取表所在的RegionServer
            Metadata.TableRegionInfo regionInfo = tableRegions.get(tableName);
            List<String> servers = regionInfo.getRegionServers();
            
            // 通知所有相关RegionServer创建索引
            boolean allSuccess = true;
            for (String server : servers) {
                try {
                    RegionService regionService = RPCUtils.getRegionService(server);
                    if (regionService != null) {
                        Message response = regionService.createIndex(indexInfo);
                        if (response.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                            allSuccess = false;
                        }
                    }
                } catch (Exception e) {
                    allSuccess = false;
                    e.printStackTrace();
                }
            }
            
            if (!allSuccess) {
                // 回滚索引创建
                for (int i = 0; i < tableInfo.getIndexes().size(); i++) {
                    if (tableInfo.getIndexes().get(i).getIndexName().equals(indexName)) {
                        tableInfo.getIndexes().remove(i);
                        break;
                    }
                }
                return Message.createErrorResponse("master", "client", "部分RegionServer创建索引失败");
            }
            
            // 更新表元数据
            String tablePath = ZKUtils.TABLES_NODE + "/" + tableName;
            zkUtils.setData(tablePath, serialize(tableInfo));
            
            return Message.createSuccessResponse("master", "client");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("master", "client", "创建索引失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message dropIndex(String indexName, String tableName) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tables.containsKey(tableName)) {
                return Message.createErrorResponse("master", "client", "表不存在: " + tableName);
            }
            
            Metadata.TableInfo tableInfo = tables.get(tableName);
            
            // 查找并移除索引
            boolean indexFound = false;
            List<Metadata.IndexInfo> indexes = tableInfo.getIndexes();
            for (int i = 0; i < indexes.size(); i++) {
                if (indexes.get(i).getIndexName().equals(indexName)) {
                    indexes.remove(i);
                    indexFound = true;
                    break;
                }
            }
            
            if (!indexFound) {
                return Message.createErrorResponse("master", "client", "索引不存在: " + indexName);
            }
            
            // 获取表所在的RegionServer
            Metadata.TableRegionInfo regionInfo = tableRegions.get(tableName);
            List<String> servers = regionInfo.getRegionServers();
            
            // 通知所有相关RegionServer删除索引
            boolean allSuccess = true;
            for (String server : servers) {
                try {
                    RegionService regionService = RPCUtils.getRegionService(server);
                    if (regionService != null) {
                        Message response = regionService.dropIndex(indexName, tableName);
                        if (response.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                            allSuccess = false;
                        }
                    }
                } catch (Exception e) {
                    allSuccess = false;
                    e.printStackTrace();
                }
            }
            
            if (!allSuccess) {
                return Message.createErrorResponse("master", "client", "部分RegionServer删除索引失败");
            }
            
            // 更新表元数据
            String tablePath = ZKUtils.TABLES_NODE + "/" + tableName;
            zkUtils.setData(tablePath, serialize(tableInfo));
            
            return Message.createSuccessResponse("master", "client");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("master", "client", "删除索引失败: " + e.getMessage());
        }
    }
    
    @Override
    public List<String> getTableRegions(String tableName) throws RemoteException {
        Metadata.TableRegionInfo regionInfo = tableRegions.get(tableName);
        if (regionInfo == null) {
            return new ArrayList<>();
        }
        return regionInfo.getRegionServers();
    }
    
    @Override
    public Message registerRegionServer(String hostname, int port) throws RemoteException {
        try {
            String regionServerKey = hostname + ":" + port;
            
            // 检查RegionServer是否已注册
            if (regionServers.containsKey(regionServerKey)) {
                return Message.createErrorResponse("master", regionServerKey, "RegionServer已注册");
            }
            
            // 注册RegionServer
            regionServers.put(regionServerKey, "active");
            
            // 创建RegionServer节点
            String nodePath = ZKUtils.REGION_SERVERS_NODE + "/" + regionServerKey;
            zkUtils.createNode(nodePath, regionServerKey.getBytes(), CreateMode.EPHEMERAL);
            
            return Message.createSuccessResponse("master", regionServerKey);
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("master", hostname + ":" + port, "注册RegionServer失败: " + e.getMessage());
        }
    }
    
    @Override
    public List<String> getAllRegionServers() throws RemoteException {
        return new ArrayList<>(regionServers.keySet());
    }
    
    @Override
    public List<Metadata.TableInfo> getAllTables() throws RemoteException {
        return new ArrayList<>(tables.values());
    }
    
    /**
     * 序列化对象
     */
    private byte[] serialize(Serializable obj) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(obj);
        objectStream.close();
        return byteStream.toByteArray();
    }
    
    /**
     * 反序列化对象
     */
    private <T> T deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
        ObjectInputStream objectStream = new ObjectInputStream(byteStream);
        T object = (T) objectStream.readObject();
        objectStream.close();
        return object;
    }
} 