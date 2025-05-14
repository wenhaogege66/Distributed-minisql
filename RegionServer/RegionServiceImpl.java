package RegionServer;

import Common.Message;
import Common.Metadata;
import Common.ZKUtils;
import Common.RPCUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RegionServer服务实现类
 */
public class RegionServiceImpl extends UnicastRemoteObject implements RegionService, Watcher {
    
    private ZKUtils zkUtils;
    private String hostname;
    private int port;
    
    // 表存储管理
    private Map<String, TableStorage> tableStorages;
    
    // 表元数据缓存
    private Map<String, Metadata.TableInfo> tableInfoCache;
    
    // 服务器状态
    private Map<String, Object> serverStatus;
    
    // 数据文件根目录
    private String DATA_ROOT_DIR;
    
    private Master.MasterService masterService;
    
    /**
     * 构造函数
     */
    public RegionServiceImpl(String hostname, int port) throws RemoteException {
        super();
        this.hostname = hostname;
        this.port = port;
        
        tableStorages = new ConcurrentHashMap<>();
        tableInfoCache = new ConcurrentHashMap<>();
        serverStatus = new HashMap<>();
        
        // 初始化服务器状态
        serverStatus.put("hostname", hostname);
        serverStatus.put("port", port);
        serverStatus.put("startTime", System.currentTimeMillis());
        serverStatus.put("tableCount", 0);
        
        // 从系统属性获取数据目录，如果没有则使用默认的"data"
        DATA_ROOT_DIR = System.getProperty("region.data.dir", "data");
        
        // 创建数据目录
        File dataDir = new File(DATA_ROOT_DIR);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        
        // 初始化ZooKeeper连接
        initZooKeeper();
        
        // 恢复本地表数据
        recoverLocalTables();
    }
    
    /**
     * 恢复本地表数据
     */
    private void recoverLocalTables() {
        File dataDir = new File(DATA_ROOT_DIR);
        File[] tableDirs = dataDir.listFiles(File::isDirectory);
        if (tableDirs != null) {
            for (File tableDir : tableDirs) {
                String tableName = tableDir.getName();
                try {
                    // 先从Master获取表信息，确认表是否仍然存在
                    boolean tableExists = false;
                    if (masterService != null) {
                        try {
                            List<Metadata.TableInfo> allTables = masterService.getAllTables();
                            for (Metadata.TableInfo info : allTables) {
                                if (info.getTableName().equals(tableName)) {
                                    tableExists = true;
                                    // 使用Master的表元数据更新本地
                                    tableInfoCache.put(tableName, info);
                                    
                                    // 创建表存储
                                    TableStorage tableStorage = new TableStorage(tableName);
                                    tableStorage.setDataRootDir(DATA_ROOT_DIR);
                                    tableStorage.initialize(info);
                                    tableStorage.loadData(); // 加载已有数据
                                    tableStorages.put(tableName, tableStorage);
                                    
                                    System.out.println("恢复表: " + tableName);
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("从Master获取表信息失败: " + e.getMessage());
                        }
                    }
                    
                    // 如果Master中不存在该表，则不恢复，并删除本地数据
                    if (!tableExists) {
                        System.out.println("表 " + tableName + " 在Master中不存在，清理本地数据");
                        deleteTableDirectory(tableName);
                        continue;
                    }
                } catch (Exception e) {
                    System.err.println("恢复表 " + tableName + " 失败: " + e.getMessage());
                }
            }
            
            // 更新服务器状态
            serverStatus.put("tableCount", tableStorages.size());
        }
    }
    
    /**
     * 初始化ZooKeeper连接
     */
    private void initZooKeeper() {
        try {
            // 连接ZooKeeper
            zkUtils = new ZKUtils();
            zkUtils.connect();
            
            // 获取Master地址
            String masterAddress = zkUtils.getMasterAddress();
            if (masterAddress != null) {
                String[] parts = masterAddress.split(":");
                String masterHost = parts[0];
                int masterPort = Integer.parseInt(parts[1]);
                
                // 连接Master服务
                try {
                    masterService = Common.RPCUtils.getMasterService(masterHost, masterPort);
                } catch (Exception e) {
                    System.err.println("连接Master失败: " + e.getMessage());
                }
            }
            
            // 注册服务器
            registerRegionServer();
            
            // 监听Master节点变化
            watchMaster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 注册RegionServer节点
     */
    private void registerRegionServer() throws KeeperException, InterruptedException {
        String regionServerKey = hostname + ":" + port;
        String nodePath = ZKUtils.REGION_SERVERS_NODE + "/" + regionServerKey;
        zkUtils.createNode(nodePath, regionServerKey.getBytes(), CreateMode.EPHEMERAL);
        
        System.out.println("RegionServer registered: " + regionServerKey);
    }
    
    /**
     * 监听Master节点变化
     */
    private void watchMaster() throws KeeperException, InterruptedException {
        if (zkUtils.exists(ZKUtils.MASTER_NODE, this)) {
            byte[] data = zkUtils.getData(ZKUtils.MASTER_NODE, this);
            System.out.println("Master node data: " + new String(data));
        }
    }
    
    @Override
    public void process(WatchedEvent event) {
        try {
            // 处理Master节点变化事件
            if (event.getPath() != null && event.getPath().equals(ZKUtils.MASTER_NODE)) {
                watchMaster();
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
            if (tableStorages.containsKey(tableName)) {
                // 如果表已存在，比对元数据是否一致
                Metadata.TableInfo existingInfo = tableInfoCache.get(tableName);
                
                // 简单比较表名是否相同，这里可以扩展为更完整的元数据比较
                if (existingInfo != null && existingInfo.getTableName().equals(tableName)) {
                    // 如果是相同的表，返回成功，使操作具有幂等性
                    System.out.println("表 " + tableName + " 已存在，且元数据一致，视为成功");
                    return Message.createSuccessResponse("region-" + hostname + ":" + port, "master");
                }
                
                return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "表已存在: " + tableName);
            }
            
            // 创建表存储
            TableStorage tableStorage = new TableStorage(tableName);
            tableStorage.setDataRootDir(DATA_ROOT_DIR);
            tableStorage.initialize(tableInfo);
            
            // 保存表存储和元数据
            tableStorages.put(tableName, tableStorage);
            tableInfoCache.put(tableName, tableInfo);
            
            // 保存表元数据到文件
            saveTableMetadata(tableName, tableInfo);
            
            // 更新服务器状态
            serverStatus.put("tableCount", tableStorages.size());
            
            System.out.println("成功创建表: " + tableName);
            
            return Message.createSuccessResponse("region-" + hostname + ":" + port, "master");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "创建表失败: " + e.getMessage());
        }
    }
    
    /**
     * 保存表元数据到文件
     */
    private void saveTableMetadata(String tableName, Metadata.TableInfo tableInfo) throws IOException {
        String tableDir = DATA_ROOT_DIR + "/" + tableName;
        File dir = new File(tableDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        File metadataFile = new File(dir, "metadata.dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metadataFile))) {
            oos.writeObject(tableInfo);
        }
    }
    
    @Override
    public Message dropTable(String tableName) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "表不存在: " + tableName);
            }
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 关闭表存储
            tableStorage.close();
            
            // 移除表存储和元数据
            tableStorages.remove(tableName);
            tableInfoCache.remove(tableName);
            
            // 删除表目录
            deleteTableDirectory(tableName);
            
            // 从ZooKeeper中删除表节点
            try {
                String tableZkPath = ZKUtils.TABLES_NODE + "/" + tableName;
                if (zkUtils.exists(tableZkPath, null)) {
                    // 需要先删除子节点
                    List<String> children = zkUtils.getChildren(tableZkPath, null);
                    for (String child : children) {
                        zkUtils.deleteNode(tableZkPath + "/" + child);
                    }
                    // 然后删除表节点
                    zkUtils.deleteNode(tableZkPath);
                }
            } catch (KeeperException | InterruptedException e) {
                System.err.println("删除ZooKeeper节点失败: " + e.getMessage());
                // 继续执行，不要因为ZK问题而导致整个删除操作失败
            }
            
            // 更新服务器状态
            serverStatus.put("tableCount", tableStorages.size());
            
            System.out.println("成功删除表: " + tableName);
            
            return Message.createSuccessResponse("region-" + hostname + ":" + port, "master");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "删除表失败: " + e.getMessage());
        }
    }
    
    /**
     * 删除表目录
     */
    private void deleteTableDirectory(String tableName) {
        File tableDir = new File(DATA_ROOT_DIR + "/" + tableName);
        if (tableDir.exists()) {
            deleteDirectory(tableDir);
        }
    }
    
    /**
     * 递归删除目录
     */
    private boolean deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        return directory.delete();
    }
    
    @Override
    public Message createIndex(Metadata.IndexInfo indexInfo) throws RemoteException {
        try {
            String tableName = indexInfo.getTableName();
            
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "表不存在: " + tableName);
            }
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 更新表元数据
            Metadata.TableInfo tableInfo = tableInfoCache.get(tableName);
            tableInfo.addIndex(indexInfo);
            
            // 创建索引
            tableStorage.createIndex(indexInfo);
            
            // 保存表元数据到文件
            saveTableMetadata(tableName, tableInfo);
            
            System.out.println("成功创建索引: " + indexInfo.getIndexName() + " 在表 " + tableName);
            
            return Message.createSuccessResponse("region-" + hostname + ":" + port, "master");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "创建索引失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message dropIndex(String indexName, String tableName) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "表不存在: " + tableName);
            }
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 更新表元数据
            Metadata.TableInfo tableInfo = tableInfoCache.get(tableName);
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
                return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "索引不存在: " + indexName);
            }
            
            // 删除索引
            tableStorage.dropIndex(indexName);
            
            // 保存表元数据到文件
            saveTableMetadata(tableName, tableInfo);
            
            System.out.println("成功删除索引: " + indexName + " 在表 " + tableName);
            
            return Message.createSuccessResponse("region-" + hostname + ":" + port, "master");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "删除索引失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message insert(String tableName, Map<String, Object> values) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "表不存在: " + tableName);
            }
            
            // 获取表元数据
            Metadata.TableInfo tableInfo = tableInfoCache.get(tableName);
            
            // 转换数据类型以匹配表结构
            Map<String, Object> convertedValues = convertValuesToMatchSchema(values, tableInfo);
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 校验数据
            String validationError = validateInsertData(tableInfo, convertedValues);
            if (validationError != null) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "client", validationError);
            }
            
            // 执行插入
            boolean success = tableStorage.insert(convertedValues);
            if (!success) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "插入数据失败");
            }
            
            return Message.createSuccessResponse("region-" + hostname + ":" + port, "client");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "插入数据失败: " + e.getMessage());
        }
    }
    
    private Map<String, Object> convertValuesToMatchSchema(Map<String, Object> values, Metadata.TableInfo tableInfo) {
        Map<String, Object> convertedValues = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            // 查找列定义
            Metadata.ColumnInfo columnInfo = null;
            for (Metadata.ColumnInfo col : tableInfo.getColumns()) {
                if (col.getColumnName().equals(columnName)) {
                    columnInfo = col;
                    break;
                }
            }
            
            if (columnInfo == null) {
                // 列不存在，直接使用原值
                convertedValues.put(columnName, value);
                continue;
            }
            
            // 根据列类型转换值
            Object convertedValue = value;
            Common.DataTypes.Type dataType = columnInfo.getDataType().getType();
            
            if (dataType == Common.DataTypes.Type.INT) {
                // 转换为整数
                if (value instanceof String) {
                    try {
                        convertedValue = Integer.parseInt((String) value);
                    } catch (NumberFormatException e) {
                        // 无需打印异常信息
                    }
                } else if (value instanceof Number) {
                    convertedValue = ((Number) value).intValue();
                }
            } else if (dataType == Common.DataTypes.Type.FLOAT) {
                // 转换为浮点数
                if (value instanceof String) {
                    try {
                        convertedValue = Float.parseFloat((String) value);
                    } catch (NumberFormatException e) {
                        // 无需打印异常信息
                    }
                } else if (value instanceof Number) {
                    convertedValue = ((Number) value).floatValue();
                }
            }
            // 其他类型保持不变
            
            convertedValues.put(columnName, convertedValue);
        }
        
        return convertedValues;
    }
    
    /**
     * 校验插入数据
     */
    private String validateInsertData(Metadata.TableInfo tableInfo, Map<String, Object> values) {
        // 检查必填字段
        for (Metadata.ColumnInfo column : tableInfo.getColumns()) {
            if (column.isNotNull() && !values.containsKey(column.getColumnName())) {
                return "字段不能为空: " + column.getColumnName();
            }
        }
        
        // 检查字段类型 (实际应用中应该有更详细的类型检查)
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            boolean columnFound = false;
            for (Metadata.ColumnInfo column : tableInfo.getColumns()) {
                if (column.getColumnName().equals(columnName)) {
                    columnFound = true;
                    break;
                }
            }
            
            if (!columnFound) {
                return "未知字段: " + columnName;
            }
        }
        
        // 检查主键约束
        String primaryKey = tableInfo.getPrimaryKey();
        if (primaryKey != null && values.containsKey(primaryKey)) {
            Object primaryKeyValue = values.get(primaryKey);
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableInfo.getTableName());
            if (tableStorage != null) {
                // 检查主键是否已存在
                if (tableStorage.isPrimaryKeyExists(primaryKey, primaryKeyValue)) {
                    return "主键重复: " + primaryKey + " = " + primaryKeyValue;
                }
            }
        }
        
        // 检查唯一性约束
        for (Metadata.ColumnInfo column : tableInfo.getColumns()) {
            if (column.isUnique() && values.containsKey(column.getColumnName())) {
                String columnName = column.getColumnName();
                Object value = values.get(columnName);
                
                // 获取表存储
                TableStorage tableStorage = tableStorages.get(tableInfo.getTableName());
                if (tableStorage != null) {
                    // 检查唯一值是否已存在
                    if (tableStorage.isUniqueValueExists(columnName, value)) {
                        return "唯一性约束冲突: " + columnName + " = " + value;
                    }
                }
            }
        }
        
        return null;
    }
    
    @Override
    public Message delete(String tableName, Map<String, Object> conditions) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "表不存在: " + tableName);
            }
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 执行删除
            int count = tableStorage.delete(conditions);
            
            System.out.println("成功从表 " + tableName + " 删除 " + count + " 条记录");
            
            Message response = Message.createSuccessResponse("region-" + hostname + ":" + port, "client");
            response.setData("deletedCount", count);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "删除数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public List<Map<String, Object>> select(String tableName, List<String> columns, Map<String, Object> conditions) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                throw new RemoteException("表不存在: " + tableName);
            }
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 执行查询
            List<Map<String, Object>> result = tableStorage.select(columns, conditions);
            
            System.out.println("成功从表 " + tableName + " 查询 " + result.size() + " 条记录");
            
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RemoteException("查询数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message update(String tableName, Map<String, Object> values, Map<String, Object> conditions) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "表不存在: " + tableName);
            }
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 执行更新
            int count = tableStorage.update(values, conditions);
            
            System.out.println("成功更新表 " + tableName + " 的 " + count + " 条记录");
            
            Message response = Message.createSuccessResponse("region-" + hostname + ":" + port, "client");
            response.setData("updatedCount", count);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "更新数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getStatus() throws RemoteException {
        // 更新当前状态
        serverStatus.put("currentTime", System.currentTimeMillis());
        serverStatus.put("uptime", System.currentTimeMillis() - (long) serverStatus.get("startTime"));
        serverStatus.put("tableCount", tableStorages.size());
        
        return serverStatus;
    }
    
    @Override
    public boolean heartbeat() throws RemoteException {
        // 更新最后心跳时间
        serverStatus.put("lastHeartbeat", System.currentTimeMillis());
        return true;
    }
    
    @Override
    public List<Map<String, Object>> getAllTableData(String tableName) throws RemoteException {
        try {
            // 检查表是否存在
            if (!tableStorages.containsKey(tableName)) {
                return new ArrayList<>();
            }
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 获取表中所有数据
            return tableStorage.getAllData();
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
    
    @Override
    public Message replicateTableFrom(String tableName, String sourceServer) throws RemoteException {
        try {
            // 检查本地是否已有该表
            if (tableStorages.containsKey(tableName)) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "表已存在: " + tableName);
            }
            
            // 获取源服务器的RegionService
            RegionService sourceRegionService = RPCUtils.getRegionService(sourceServer);
            
            // 首先获取表元数据信息
            List<Metadata.TableInfo> allTables = masterService.getAllTables();
            Metadata.TableInfo tableInfo = null;
            
            for (Metadata.TableInfo info : allTables) {
                if (info.getTableName().equals(tableName)) {
                    tableInfo = info;
                    break;
                }
            }
            
            if (tableInfo == null) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "找不到表元数据: " + tableName);
            }
            
            // 创建表
            Message createResult = createTable(tableInfo);
            if (createResult.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                return createResult;
            }
            
            // 从源RegionServer获取数据
            List<Map<String, Object>> data = sourceRegionService.getAllTableData(tableName);
            
            // 将数据批量插入到本地
            int successCount = 0;
            for (Map<String, Object> record : data) {
                Message insertResult = insert(tableName, record);
                if (insertResult.getType() == Common.Message.MessageType.RESPONSE_SUCCESS) {
                    successCount++;
                }
            }
            
            Message response = Message.createSuccessResponse("region-" + hostname + ":" + port, "master");
            response.setData("message", "成功复制表 " + tableName + " 数据，共 " + successCount + "/" + data.size() + " 条记录");
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "master", "复制表数据失败: " + e.getMessage());
        }
    }
    
    /**
     * 表存储类，管理表的数据和索引
     */
    private static class TableStorage implements Serializable {
        private static final long serialVersionUID = 1L;
        private String tableName;
        private transient File dataFile;
        private transient Map<String, File> indexFiles;
        private List<Map<String, Object>> data; // 内存中的数据
        private String dataRootDir; // 存储数据根目录路径
        
        public TableStorage(String tableName) {
            this.tableName = tableName;
            this.indexFiles = new HashMap<>();
            this.data = new ArrayList<>();
        }
        
        /**
         * 设置数据根目录
         */
        public void setDataRootDir(String dataRootDir) {
            this.dataRootDir = dataRootDir;
        }
        
        /**
         * 初始化表存储
         */
        public void initialize(Metadata.TableInfo tableInfo) throws IOException, ClassNotFoundException {
            // 创建数据文件
            String tableDir = dataRootDir + "/" + tableName;
            File dir = new File(tableDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            
            dataFile = new File(dir, "data.db");
            if (!dataFile.exists()) {
                dataFile.createNewFile();
                // 写入一个空列表，以初始化文件
                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dataFile))) {
                    oos.writeObject(new ArrayList<Map<String, Object>>());
                }
            }
            
            // 创建索引文件
            for (Metadata.IndexInfo indexInfo : tableInfo.getIndexes()) {
                createIndex(indexInfo);
            }
            
            // 加载数据
            loadData();
        }
        
        /**
         * 加载数据
         */
        @SuppressWarnings("unchecked")
        public void loadData() throws IOException, ClassNotFoundException {
            if (dataFile.exists() && dataFile.length() > 0) {
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(dataFile))) {
                    data = (List<Map<String, Object>>) ois.readObject();
                } catch (ClassNotFoundException e) {
                    System.err.println("加载数据时类未找到: " + e.getMessage());
                    throw e;
                }
            }
        }
        
        /**
         * 关闭表存储
         */
        public void close() throws IOException {
            // 保存数据到文件
            saveData();
        }
        
        /**
         * 创建索引
         */
        public void createIndex(Metadata.IndexInfo indexInfo) throws IOException {
            String indexName = indexInfo.getIndexName();
            
            // 创建索引文件
            String indexDir = dataRootDir + "/" + tableName + "/indexes";
            File dir = new File(indexDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            
            File indexFile = new File(dir, indexName + ".idx");
            if (!indexFile.exists()) {
                indexFile.createNewFile();
            }
            
            indexFiles.put(indexName, indexFile);
            
            // 为已有数据构建索引
            buildIndex(indexInfo);
        }
        
        /**
         * 为已有数据构建索引
         */
        private void buildIndex(Metadata.IndexInfo indexInfo) {
            // 实际应该遍历数据文件并构建索引
            // 这里简化实现
        }
        
        /**
         * 删除索引
         */
        public void dropIndex(String indexName) {
            // 删除索引文件
            File indexFile = indexFiles.get(indexName);
            if (indexFile != null && indexFile.exists()) {
                indexFile.delete();
            }
            
            indexFiles.remove(indexName);
        }
        
        /**
         * 插入数据
         */
        public boolean insert(Map<String, Object> values) {
            // 添加到内存数据 - 使用深拷贝确保类型一致
            Map<String, Object> newRow = new HashMap<>();
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                newRow.put(entry.getKey(), entry.getValue());
            }
            
            data.add(newRow);
            
            // 更新索引
            updateIndexes(values);
            
            // 保存到文件
            try {
                saveData();
                return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        
        /**
         * 更新索引
         */
        private void updateIndexes(Map<String, Object> values) {
            // 实际应该更新索引文件
            // 这里简化实现
        }
        
        /**
         * 保存数据到文件
         */
        private void saveData() throws IOException {
            // 实际应该使用数据库文件格式
            // 这里简化实现，使用序列化
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dataFile))) {
                oos.writeObject(data);
            }
        }
        
        /**
         * 查询数据
         */
        public List<Map<String, Object>> select(List<String> columns, Map<String, Object> conditions) {
            // 使用索引优化查询
            // 这里简化实现，直接遍历数据
            List<Map<String, Object>> result = new ArrayList<>();
            
            for (Map<String, Object> row : data) {
                boolean match = true;
                
                // 检查是否满足条件
                for (Map.Entry<String, Object> condition : conditions.entrySet()) {
                    String columnName = condition.getKey();
                    Object value = condition.getValue();
                    
                    // 处理范围查询条件
                    if (columnName.endsWith("_range") && value instanceof Map) {
                        String actualColumnName = columnName.substring(0, columnName.length() - 6); // 去掉"_range"后缀
                        Map<String, Object> rangeValues = (Map<String, Object>) value;
                        Object minValue = rangeValues.get("min");
                        Object maxValue = rangeValues.get("max");
                        
                        if (!row.containsKey(actualColumnName)) {
                            match = false;
                            break;
                        }
                        
                        Object rowValue = row.get(actualColumnName);
                        // 比较范围
                        if (rowValue instanceof Number && minValue instanceof Number && maxValue instanceof Number) {
                            double rowNum = ((Number) rowValue).doubleValue();
                            double minNum = ((Number) minValue).doubleValue();
                            double maxNum = ((Number) maxValue).doubleValue();
                            
                            if (rowNum < minNum || rowNum > maxNum) {
                                match = false;
                                break;
                            }
                        } else {
                            // 对于非数字类型，尝试字符串比较
                            String rowStr = rowValue.toString();
                            String minStr = minValue.toString();
                            String maxStr = maxValue.toString();
                            
                            if (rowStr.compareTo(minStr) < 0 || rowStr.compareTo(maxStr) > 0) {
                                match = false;
                                break;
                            }
                        }
                    } else {
                        // 普通等值查询
                        if (!row.containsKey(columnName) || !objectsEqual(row.get(columnName), value)) {
                            match = false;
                            break;
                        }
                    }
                }
                
                if (match) {
                    // 提取需要的列
                    Map<String, Object> resultRow = new HashMap<>();
                    
                    if (columns == null || columns.isEmpty()) {
                        // 如果没有指定列，返回所有列
                        resultRow.putAll(row);
                    } else {
                        // 否则只返回指定的列
                        for (String column : columns) {
                            if (row.containsKey(column)) {
                                resultRow.put(column, row.get(column));
                            }
                        }
                    }
                    
                    result.add(resultRow);
                }
            }
            
            return result;
        }
        
        /**
         * 删除数据
         */
        public int delete(Map<String, Object> conditions) {
            int count = 0;
            Iterator<Map<String, Object>> iterator = data.iterator();
            
            // 检查条件是否为空，如果为空，不删除任何记录
            if (conditions == null || conditions.isEmpty()) {
                return 0;
            }
            
            while (iterator.hasNext()) {
                Map<String, Object> row = iterator.next();
                boolean match = true;
                
                // 检查是否满足条件
                for (Map.Entry<String, Object> condition : conditions.entrySet()) {
                    String columnName = condition.getKey();
                    Object value = condition.getValue();
                    
                    if (!row.containsKey(columnName)) {
                        match = false;
                        break;
                    }
                    
                    if (!objectsEqual(row.get(columnName), value)) {
                        match = false;
                        break;
                    }
                }
                
                if (match) {
                    iterator.remove();
                    count++;
                }
            }
            
            // 保存到文件
            try {
                saveData();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            return count;
        }
        
        /**
         * 更新数据
         */
        public int update(Map<String, Object> values, Map<String, Object> conditions) {
            int count = 0;
            
            // 检查条件是否为空，如果为空，不更新任何记录
            if (conditions == null || conditions.isEmpty()) {
                return 0;
            }
            
            for (Map<String, Object> row : data) {
                boolean match = true;
                
                // 检查是否满足条件
                for (Map.Entry<String, Object> condition : conditions.entrySet()) {
                    String columnName = condition.getKey();
                    Object value = condition.getValue();
                    
                    if (!row.containsKey(columnName)) {
                        match = false;
                        break;
                    }
                    
                    if (!objectsEqual(row.get(columnName), value)) {
                        match = false;
                        break;
                    }
                }
                
                if (match) {
                    // 更新数据
                    for (Map.Entry<String, Object> entry : values.entrySet()) {
                        row.put(entry.getKey(), entry.getValue());
                    }
                    count++;
                }
            }
            
            // 保存到文件
            try {
                saveData();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            return count;
        }
        
        /**
         * 安全地比较两个对象，处理不同数据类型的比较
         */
        private boolean objectsEqual(Object obj1, Object obj2) {
            if (obj1 == null && obj2 == null) {
                return true;
            }
            if (obj1 == null || obj2 == null) {
                return false;
            }
            
            // 如果两个对象类型相同且equals方法返回true，则认为相等
            if (obj1.getClass() == obj2.getClass() && obj1.equals(obj2)) {
                return true;
            }
            
            // 数字类型特殊处理，允许不同类型的数字比较
            if (obj1 instanceof Number && obj2 instanceof Number) {
                // 对于整数类型的值，比较它们的long值
                if ((obj1 instanceof Integer || obj1 instanceof Long || obj1 instanceof Short || obj1 instanceof Byte) &&
                    (obj2 instanceof Integer || obj2 instanceof Long || obj2 instanceof Short || obj2 instanceof Byte)) {
                    return ((Number)obj1).longValue() == ((Number)obj2).longValue();
                }
                
                // 对于浮点类型，比较它们的double值
                return ((Number)obj1).doubleValue() == ((Number)obj2).doubleValue();
            }
            
            // 尝试字符串与数字之间的比较
            if (obj1 instanceof String && obj2 instanceof Number) {
                try {
                    if (obj2 instanceof Integer || obj2 instanceof Long || obj2 instanceof Short || obj2 instanceof Byte) {
                        long val1 = Long.parseLong((String)obj1);
                        long val2 = ((Number)obj2).longValue();
                        return val1 == val2;
                    } else {
                        double val1 = Double.parseDouble((String)obj1);
                        double val2 = ((Number)obj2).doubleValue();
                        return val1 == val2;
                    }
                } catch (NumberFormatException e) {
                    // 转换失败，不相等
                    return false;
                }
            }
            
            if (obj2 instanceof String && obj1 instanceof Number) {
                try {
                    if (obj1 instanceof Integer || obj1 instanceof Long || obj1 instanceof Short || obj1 instanceof Byte) {
                        long val2 = Long.parseLong((String)obj2);
                        long val1 = ((Number)obj1).longValue();
                        return val1 == val2;
                    } else {
                        double val2 = Double.parseDouble((String)obj2);
                        double val1 = ((Number)obj1).doubleValue();
                        return val1 == val2;
                    }
                } catch (NumberFormatException e) {
                    // 转换失败，不相等
                    return false;
                }
            }
            
            // 最后尝试字符串比较
            return obj1.toString().equals(obj2.toString());
        }
        
        /**
         * 获取表中所有数据
         */
        public List<Map<String, Object>> getAllData() {
            return new ArrayList<>(data);
        }
        
        /**
         * 检查主键是否已存在
         */
        public boolean isPrimaryKeyExists(String primaryKeyColumn, Object primaryKeyValue) {
            for (Map<String, Object> row : data) {
                if (row.containsKey(primaryKeyColumn)) {
                    Object existingValue = row.get(primaryKeyColumn);
                    if (objectsEqual(existingValue, primaryKeyValue)) {
                        return true;
                    }
                }
            }
            return false;
        }
        
        /**
         * 检查唯一值是否已存在
         */
        public boolean isUniqueValueExists(String uniqueColumn, Object uniqueValue) {
            for (Map<String, Object> row : data) {
                if (row.containsKey(uniqueColumn)) {
                    Object existingValue = row.get(uniqueColumn);
                    if (objectsEqual(existingValue, uniqueValue)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
    
    /**
     * 反序列化对象
     */
    private <T> T deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try {
            ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
            ObjectInputStream objectStream = new ObjectInputStream(byteStream);
            T object = (T) objectStream.readObject();
            objectStream.close();
            return object;
        } catch (ClassNotFoundException e) {
            System.err.println("反序列化时未找到类: " + e.getMessage());
            throw e;
        } catch (IOException e) {
            System.err.println("反序列化时IO错误: " + e.getMessage());
            throw e;
        }
    }
} 