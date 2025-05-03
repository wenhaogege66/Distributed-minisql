package Client;

import Common.Message;
import Common.Metadata;
import Master.MasterService;
import RegionServer.RegionService;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 客户端服务实现类
 */
public class ClientServiceImpl implements ClientService {
    
    private MasterService masterService;
    private Map<String, RegionService> regionServices;
    private Map<String, List<String>> tableRegions; // tableName -> regionServers
    
    /**
     * 构造函数
     */
    public ClientServiceImpl() {
        regionServices = new HashMap<>();
        tableRegions = new HashMap<>();
    }
    
    @Override
    public boolean connect(String masterHost, int masterPort) {
        try {
            // 连接到Master
            Registry registry = LocateRegistry.getRegistry(masterHost, masterPort);
            masterService = (MasterService) registry.lookup("MasterService");
            
            System.out.println("Connected to Master at " + masterHost + ":" + masterPort);
            return true;
        } catch (Exception e) {
            System.err.println("Client connect exception: " + e.toString());
            e.printStackTrace();
            return false;
        }
    }
    
    @Override
    public void disconnect() {
        masterService = null;
        regionServices.clear();
        tableRegions.clear();
        System.out.println("Disconnected from server");
    }
    
    @Override
    public Message executeSql(String sql) {
        try {
            // 简单的SQL解析
            if (sql.toUpperCase().startsWith("CREATE TABLE")) {
                return executeCreateTable(sql);
            } else if (sql.toUpperCase().startsWith("DROP TABLE")) {
                return executeDropTable(sql);
            } else if (sql.toUpperCase().startsWith("CREATE INDEX")) {
                return executeCreateIndex(sql);
            } else if (sql.toUpperCase().startsWith("DROP INDEX")) {
                return executeDropIndex(sql);
            } else if (sql.toUpperCase().startsWith("INSERT INTO")) {
                return executeInsert(sql);
            } else if (sql.toUpperCase().startsWith("DELETE FROM")) {
                return executeDelete(sql);
            } else if (sql.toUpperCase().startsWith("SELECT")) {
                executeSelect(sql);
                return Message.createSuccessResponse("client", "user");
            } else if (sql.toUpperCase().startsWith("UPDATE")) {
                return executeUpdate(sql);
            } else {
                return Message.createErrorResponse("client", "user", "不支持的SQL语句: " + sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "执行SQL失败: " + e.getMessage());
        }
    }
    
    /**
     * 执行CREATE TABLE语句
     */
    private Message executeCreateTable(String sql) throws RemoteException {
        // 解析CREATE TABLE语句
        // 这里是一个简化的解析器，实际应用中应该更复杂
        Pattern pattern = Pattern.compile("CREATE TABLE (\\w+) \\((.+)\\);?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        String columnsStr = matcher.group(2);
        
        // 解析列定义
        String[] columnDefs = columnsStr.split(",");
        List<Metadata.ColumnInfo> columns = new ArrayList<>();
        String primaryKey = null;
        
        for (String columnDef : columnDefs) {
            columnDef = columnDef.trim();
            
            // 检查是否是PRIMARY KEY定义
            if (columnDef.toUpperCase().startsWith("PRIMARY KEY")) {
                Pattern pkPattern = Pattern.compile("PRIMARY KEY\\s*\\(\\s*(\\w+)\\s*\\)", Pattern.CASE_INSENSITIVE);
                Matcher pkMatcher = pkPattern.matcher(columnDef);
                if (pkMatcher.find()) {
                    primaryKey = pkMatcher.group(1);
                }
                continue;
            }
            
            // 解析列名和类型
            Pattern colPattern = Pattern.compile("(\\w+)\\s+(\\w+(?:\\s*\\(\\s*\\d+\\s*\\))?)(\\s+NOT NULL)?(\\s+UNIQUE)?", Pattern.CASE_INSENSITIVE);
            Matcher colMatcher = colPattern.matcher(columnDef);
            
            if (colMatcher.find()) {
                String columnName = colMatcher.group(1);
                String dataTypeStr = colMatcher.group(2).toUpperCase();
                boolean notNull = colMatcher.group(3) != null;
                boolean unique = colMatcher.group(4) != null;
                
                // 解析数据类型
                Metadata.ColumnInfo columnInfo = createColumnInfo(columnName, dataTypeStr, notNull, unique);
                columns.add(columnInfo);
            }
        }
        
        // 创建表信息
        Metadata.TableInfo tableInfo = new Metadata.TableInfo(tableName);
        for (Metadata.ColumnInfo column : columns) {
            tableInfo.addColumn(column);
        }
        
        if (primaryKey != null) {
            tableInfo.setPrimaryKey(primaryKey);
        }
        
        // 调用Master创建表
        return createTable(tableInfo);
    }
    
    /**
     * 创建列信息
     */
    private Metadata.ColumnInfo createColumnInfo(String columnName, String dataTypeStr, boolean notNull, boolean unique) {
        Common.DataTypes.DataType dataType;
        
        if (dataTypeStr.startsWith("INT")) {
            dataType = new Common.DataTypes.IntType();
        } else if (dataTypeStr.startsWith("FLOAT")) {
            dataType = new Common.DataTypes.FloatType();
        } else if (dataTypeStr.startsWith("CHAR")) {
            Pattern pattern = Pattern.compile("CHAR\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(dataTypeStr);
            int length = 1;
            if (matcher.find()) {
                length = Integer.parseInt(matcher.group(1));
            }
            dataType = new Common.DataTypes.CharType(length);
        } else if (dataTypeStr.startsWith("VARCHAR")) {
            Pattern pattern = Pattern.compile("VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(dataTypeStr);
            int maxLength = 255;
            if (matcher.find()) {
                maxLength = Integer.parseInt(matcher.group(1));
            }
            dataType = new Common.DataTypes.VarcharType(maxLength);
        } else {
            // 默认使用VARCHAR
            dataType = new Common.DataTypes.VarcharType(255);
        }
        
        return new Metadata.ColumnInfo(columnName, dataType, notNull, unique);
    }
    
    /**
     * 执行DROP TABLE语句
     */
    private Message executeDropTable(String sql) throws RemoteException {
        // 解析DROP TABLE语句
        Pattern pattern = Pattern.compile("DROP TABLE (\\w+);?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        
        // 调用Master删除表
        return dropTable(tableName);
    }
    
    /**
     * 执行CREATE INDEX语句
     */
    private Message executeCreateIndex(String sql) throws RemoteException {
        // 解析CREATE INDEX语句
        Pattern pattern = Pattern.compile("CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(\\w+)\\s+ON\\s+(\\w+)\\s*\\(\\s*(\\w+)\\s*\\);?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        boolean unique = matcher.group(1) != null;
        String indexName = matcher.group(2);
        String tableName = matcher.group(3);
        String columnName = matcher.group(4);
        
        // 创建索引信息
        Metadata.IndexInfo indexInfo = new Metadata.IndexInfo(indexName, tableName, columnName, unique);
        
        // 调用Master创建索引
        return createIndex(indexInfo);
    }
    
    /**
     * 执行DROP INDEX语句
     */
    private Message executeDropIndex(String sql) throws RemoteException {
        // 解析DROP INDEX语句
        Pattern pattern = Pattern.compile("DROP\\s+INDEX\\s+(\\w+)\\s+ON\\s+(\\w+);?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String indexName = matcher.group(1);
        String tableName = matcher.group(2);
        
        // 调用Master删除索引
        return dropIndex(indexName, tableName);
    }
    
    /**
     * 执行INSERT语句
     */
    private Message executeInsert(String sql) throws RemoteException {
        // 解析INSERT语句
        Pattern pattern = Pattern.compile("INSERT\\s+INTO\\s+(\\w+)\\s*\\((.+?)\\)\\s*VALUES\\s*\\((.+?)\\);?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        String columnsStr = matcher.group(2);
        String valuesStr = matcher.group(3);
        
        // 解析列名和值
        String[] columns = columnsStr.split(",");
        String[] values = valuesStr.split(",");
        
        if (columns.length != values.length) {
            return Message.createErrorResponse("client", "user", "列数与值数不匹配");
        }
        
        // 构建插入数据
        Map<String, Object> data = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i].trim();
            String value = values[i].trim();
            
            // 去掉引号
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            } else if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }
            
            data.put(column, value);
        }
        
        // 调用RegionServer插入数据
        return insert(tableName, data);
    }
    
    /**
     * 执行DELETE语句
     */
    private Message executeDelete(String sql) throws RemoteException {
        // 解析DELETE语句
        Pattern pattern = Pattern.compile("DELETE\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+?))?;?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        String whereClause = matcher.group(2);
        
        // 解析WHERE条件
        Map<String, Object> conditions = new HashMap<>();
        if (whereClause != null) {
            String[] conditionParts = whereClause.split("AND");
            for (String condition : conditionParts) {
                condition = condition.trim();
                String[] parts = condition.split("=");
                if (parts.length == 2) {
                    String column = parts[0].trim();
                    String value = parts[1].trim();
                    
                    // 去掉引号
                    if (value.startsWith("'") && value.endsWith("'")) {
                        value = value.substring(1, value.length() - 1);
                    } else if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    
                    conditions.put(column, value);
                }
            }
        }
        
        // 调用RegionServer删除数据
        return delete(tableName, conditions);
    }
    
    /**
     * 执行SELECT语句
     */
    private void executeSelect(String sql) throws RemoteException {
        // 解析SELECT语句
        Pattern pattern = Pattern.compile("SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+?))?;?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            throw new RemoteException("SQL语法错误: " + sql);
        }
        
        String columnsStr = matcher.group(1);
        String tableName = matcher.group(2);
        String whereClause = matcher.group(3);
        
        // 解析列名
        List<String> columns = new ArrayList<>();
        if (!columnsStr.equals("*")) {
            String[] columnParts = columnsStr.split(",");
            for (String column : columnParts) {
                columns.add(column.trim());
            }
        }
        
        // 解析WHERE条件
        Map<String, Object> conditions = new HashMap<>();
        if (whereClause != null) {
            String[] conditionParts = whereClause.split("AND");
            for (String condition : conditionParts) {
                condition = condition.trim();
                String[] parts = condition.split("=");
                if (parts.length == 2) {
                    String column = parts[0].trim();
                    String value = parts[1].trim();
                    
                    // 去掉引号
                    if (value.startsWith("'") && value.endsWith("'")) {
                        value = value.substring(1, value.length() - 1);
                    } else if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    
                    conditions.put(column, value);
                }
            }
        }
        
        // 调用RegionServer查询数据
        List<Map<String, Object>> result = select(tableName, columns, conditions);
        
        // 打印结果
        printResult(result);
    }
    
    /**
     * 打印查询结果
     */
    private void printResult(List<Map<String, Object>> result) {
        if (result.isEmpty()) {
            System.out.println("查询结果为空");
            return;
        }
        
        // 获取所有列名
        Set<String> allColumns = new HashSet<>();
        for (Map<String, Object> row : result) {
            allColumns.addAll(row.keySet());
        }
        
        // 打印表头
        for (String column : allColumns) {
            System.out.print(column + "\t");
        }
        System.out.println();
        
        // 打印分割线
        for (int i = 0; i < allColumns.size() * 8; i++) {
            System.out.print("-");
        }
        System.out.println();
        
        // 打印数据
        for (Map<String, Object> row : result) {
            for (String column : allColumns) {
                Object value = row.get(column);
                System.out.print((value != null ? value.toString() : "NULL") + "\t");
            }
            System.out.println();
        }
    }
    
    /**
     * 执行UPDATE语句
     */
    private Message executeUpdate(String sql) throws RemoteException {
        // 解析UPDATE语句
        Pattern pattern = Pattern.compile("UPDATE\\s+(\\w+)\\s+SET\\s+(.+?)(?:\\s+WHERE\\s+(.+?))?;?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        String setClause = matcher.group(2);
        String whereClause = matcher.group(3);
        
        // 解析SET子句
        Map<String, Object> values = new HashMap<>();
        String[] setParts = setClause.split(",");
        for (String setPart : setParts) {
            setPart = setPart.trim();
            String[] parts = setPart.split("=");
            if (parts.length == 2) {
                String column = parts[0].trim();
                String value = parts[1].trim();
                
                // 去掉引号
                if (value.startsWith("'") && value.endsWith("'")) {
                    value = value.substring(1, value.length() - 1);
                } else if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }
                
                values.put(column, value);
            }
        }
        
        // 解析WHERE条件
        Map<String, Object> conditions = new HashMap<>();
        if (whereClause != null) {
            String[] conditionParts = whereClause.split("AND");
            for (String condition : conditionParts) {
                condition = condition.trim();
                String[] parts = condition.split("=");
                if (parts.length == 2) {
                    String column = parts[0].trim();
                    String value = parts[1].trim();
                    
                    // 去掉引号
                    if (value.startsWith("'") && value.endsWith("'")) {
                        value = value.substring(1, value.length() - 1);
                    } else if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    
                    conditions.put(column, value);
                }
            }
        }
        
        // 调用RegionServer更新数据
        return update(tableName, values, conditions);
    }
    
    @Override
    public Message createTable(Metadata.TableInfo tableInfo) {
        try {
            // 调用Master创建表
            Message response = masterService.createTable(tableInfo);
            
            // 更新表区域信息
            updateTableRegions(tableInfo.getTableName());
            
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "创建表失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message dropTable(String tableName) {
        try {
            // 调用Master删除表
            Message response = masterService.dropTable(tableName);
            
            // 清除表区域信息
            tableRegions.remove(tableName);
            
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "删除表失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message createIndex(Metadata.IndexInfo indexInfo) {
        try {
            // 调用Master创建索引
            return masterService.createIndex(indexInfo);
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "创建索引失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message dropIndex(String indexName, String tableName) {
        try {
            // 调用Master删除索引
            return masterService.dropIndex(indexName, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "删除索引失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message insert(String tableName, Map<String, Object> values) {
        try {
            // 获取表区域信息
            if (!tableRegions.containsKey(tableName)) {
                updateTableRegions(tableName);
            }
            
            List<String> servers = tableRegions.get(tableName);
            if (servers == null || servers.isEmpty()) {
                return Message.createErrorResponse("client", "user", "找不到表所在的RegionServer: " + tableName);
            }
            
            // 选择第一个RegionServer
            String regionServer = servers.get(0);
            RegionService regionService = getRegionService(regionServer);
            
            // 调用RegionServer插入数据
            return regionService.insert(tableName, values);
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "插入数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public Message delete(String tableName, Map<String, Object> conditions) {
        try {
            // 获取表区域信息
            if (!tableRegions.containsKey(tableName)) {
                updateTableRegions(tableName);
            }
            
            List<String> servers = tableRegions.get(tableName);
            if (servers == null || servers.isEmpty()) {
                return Message.createErrorResponse("client", "user", "找不到表所在的RegionServer: " + tableName);
            }
            
            // 选择第一个RegionServer
            String regionServer = servers.get(0);
            RegionService regionService = getRegionService(regionServer);
            
            // 调用RegionServer删除数据
            return regionService.delete(tableName, conditions);
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "删除数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public List<Map<String, Object>> select(String tableName, List<String> columns, Map<String, Object> conditions) {
        try {
            // 获取表区域信息
            if (!tableRegions.containsKey(tableName)) {
                updateTableRegions(tableName);
            }
            
            List<String> servers = tableRegions.get(tableName);
            if (servers == null || servers.isEmpty()) {
                throw new RemoteException("找不到表所在的RegionServer: " + tableName);
            }
            
            // 选择第一个RegionServer
            String regionServer = servers.get(0);
            RegionService regionService = getRegionService(regionServer);
            
            // 调用RegionServer查询数据
            return regionService.select(tableName, columns, conditions);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("查询数据失败: " + e.getMessage());
            return new ArrayList<>();
        }
    }
    
    @Override
    public Message update(String tableName, Map<String, Object> values, Map<String, Object> conditions) {
        try {
            // 获取表区域信息
            if (!tableRegions.containsKey(tableName)) {
                updateTableRegions(tableName);
            }
            
            List<String> servers = tableRegions.get(tableName);
            if (servers == null || servers.isEmpty()) {
                return Message.createErrorResponse("client", "user", "找不到表所在的RegionServer: " + tableName);
            }
            
            // 选择第一个RegionServer
            String regionServer = servers.get(0);
            RegionService regionService = getRegionService(regionServer);
            
            // 调用RegionServer更新数据
            return regionService.update(tableName, values, conditions);
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "更新数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public List<Metadata.TableInfo> getAllTables() {
        try {
            // 调用Master获取所有表信息
            return masterService.getAllTables();
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
    
    @Override
    public List<String> getAllRegionServers() {
        try {
            // 调用Master获取所有RegionServer信息
            return masterService.getAllRegionServers();
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
    
    /**
     * 获取RegionService实例
     */
    private RegionService getRegionService(String regionServer) throws Exception {
        if (regionServices.containsKey(regionServer)) {
            return regionServices.get(regionServer);
        }
        
        // 解析主机名和端口
        String[] parts = regionServer.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        // 连接到RegionServer
        Registry registry = LocateRegistry.getRegistry(host, port);
        RegionService regionService = (RegionService) registry.lookup("RegionService");
        
        // 缓存RegionService实例
        regionServices.put(regionServer, regionService);
        
        return regionService;
    }
    
    /**
     * 更新表区域信息
     */
    private void updateTableRegions(String tableName) throws RemoteException {
        List<String> regions = masterService.getTableRegions(tableName);
        tableRegions.put(tableName, regions);
    }
} 