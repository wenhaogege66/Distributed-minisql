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
    private static final String DATA_ROOT_DIR = "data";
    
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
            
            // 获取表存储
            TableStorage tableStorage = tableStorages.get(tableName);
            
            // 获取表元数据
            Metadata.TableInfo tableInfo = tableInfoCache.get(tableName);
            
            // 校验数据
            String validationError = validateInsertData(tableInfo, values);
            if (validationError != null) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "client", validationError);
            }
            
            // 执行插入
            boolean success = tableStorage.insert(values);
            if (!success) {
                return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "插入数据失败");
            }
            
            System.out.println("成功插入数据到表: " + tableName);
            
            return Message.createSuccessResponse("region-" + hostname + ":" + port, "client");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("region-" + hostname + ":" + port, "client", "插入数据失败: " + e.getMessage());
        }
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
        
        public TableStorage(String tableName) {
            this.tableName = tableName;
            this.indexFiles = new HashMap<>();
            this.data = new ArrayList<>();
        }
        
        /**
         * 初始化表存储
         */
        public void initialize(Metadata.TableInfo tableInfo) throws IOException, ClassNotFoundException {
            // 创建数据文件
            String tableDir = DATA_ROOT_DIR + "/" + tableName;
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
            String indexDir = DATA_ROOT_DIR + "/" + tableName + "/indexes";
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
            // 添加到内存数据
            data.add(new HashMap<>(values));
            
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
                    
                    if (!row.containsKey(columnName) || !row.get(columnName).equals(value)) {
                        match = false;
                        break;
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
                    
                    if (!row.containsKey(columnName) || !objectsEqual(row.get(columnName), value)) {
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
                    
                    if (!row.containsKey(columnName) || !objectsEqual(row.get(columnName), value)) {
                        match = false;
                        break;
                    }
                }
                
                if (match) {
                    // 更新数据
                    row.putAll(values);
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
            
            // 如果两个对象类型相同，直接比较
            if (obj1.getClass() == obj2.getClass()) {
                return obj1.equals(obj2);
            }
            
            // 处理数值类型的比较
            if (obj1 instanceof Number && obj2 instanceof Number) {
                // 转换为 double 进行比较
                double num1 = ((Number) obj1).doubleValue();
                double num2 = ((Number) obj2).doubleValue();
                return Math.abs(num1 - num2) < 0.0001; // 允许浮点数有小误差
            }
            
            // 尝试字符串比较
            return obj1.toString().equals(obj2.toString());
        }
        
        /**
         * 获取表中所有数据
         */
        public List<Map<String, Object>> getAllData() {
            return new ArrayList<>(data);
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