package Client;

import Common.Message;
import Common.Metadata;
import Common.RPCUtils;
import Master.MasterService;
import RegionServer.RegionService;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    private Map<String, List<String>> tableRegions; // tableName -> regionServers
    
    /**
     * 构造函数
     */
    public ClientServiceImpl() {
        tableRegions = new HashMap<>();
    }
    
    @Override
    public boolean connect(String masterHost, int masterPort) {
        try {
            // 连接到Master
            masterService = RPCUtils.getMasterService(masterHost, masterPort);
            
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
        tableRegions.clear();
        RPCUtils.clearCache();
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
            } else if (sql.toUpperCase().startsWith("SELECT FROM SHARD")) {
                executeSelectFromShard(sql);
                return Message.createSuccessResponse("client", "user");
            } else if (sql.toUpperCase().startsWith("SELECT")) {
                executeSelect(sql);
                return Message.createSuccessResponse("client", "user");
            } else if (sql.toUpperCase().startsWith("UPDATE")) {
                return executeUpdate(sql);
            } else if (sql.toUpperCase().startsWith("SOURCE")) {
                return executeSourceFile(sql);
            } else if (sql.toUpperCase().startsWith("IMPORT")) {
                return executeImportData(sql);
            } else if (sql.toUpperCase().startsWith("DESCRIBE") || sql.toUpperCase().startsWith("DESC")) {
                return executeDescribe(sql);
            } else if (sql.toUpperCase().startsWith("SHOW SHARDS")) {
                return executeShowShards(sql);
            } else if (sql.toUpperCase().startsWith("SHOW SERVERS")) {
                return executeShowServers();
            } else {
                return Message.createErrorResponse("client", "user", "不支持的SQL语句: " + sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "执行SQL失败: " + e.getMessage());
        }
    }
    
    /**
     * 执行从文件导入SQL语句命令
     */
    private Message executeSourceFile(String sql) {
        try {
            // 解析文件路径
            String filePath = sql.substring("SOURCE".length()).trim();
            
            // 去掉可能存在的引号
            if ((filePath.startsWith("\"") && filePath.endsWith("\"")) || 
                (filePath.startsWith("'") && filePath.endsWith("'"))) {
                filePath = filePath.substring(1, filePath.length() - 1);
            }
            
            // 检查文件是否存在
            File file = new File(filePath);
            if (!file.exists() || !file.isFile()) {
                return Message.createErrorResponse("client", "user", "文件不存在：" + filePath);
            }
            
            // 读取文件内容
            List<String> sqlStatements = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                StringBuilder currentSql = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    
                    // 跳过空行和注释
                    if (line.isEmpty() || line.startsWith("--") || line.startsWith("#")) {
                        continue;
                    }
                    
                    currentSql.append(" ").append(line);
                    
                    // 如果行以分号结尾，则认为是一条完整的SQL语句
                    if (line.endsWith(";")) {
                        sqlStatements.add(currentSql.toString().trim());
                        currentSql = new StringBuilder();
                    }
                }
                
                // 处理最后一条可能没有分号的语句
                if (currentSql.length() > 0) {
                    sqlStatements.add(currentSql.toString().trim());
                }
            }
            
            // 依次执行所有SQL语句
            List<String> results = new ArrayList<>();
            int successCount = 0;
            
            for (String stmt : sqlStatements) {
                Message result = executeSql(stmt);
                if (result.getType() == Common.Message.MessageType.RESPONSE_SUCCESS) {
                    successCount++;
                    results.add("执行成功: " + stmt);
                } else {
                    String errorMsg = result.getData("error") != null ? result.getData("error").toString() : "未知错误";
                    results.add("执行失败: " + stmt + " - " + errorMsg);
                }
            }
            
            // 输出执行结果
            for (String result : results) {
                System.out.println(result);
            }
            
            // 返回执行概要
            Message response = Message.createSuccessResponse("client", "user");
            response.setData("message", "从文件执行SQL语句完成，成功: " + successCount + "/" + sqlStatements.size());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "执行SOURCE命令失败: " + e.getMessage());
        }
    }
    
    /**
     * 执行数据导入命令
     */
    private Message executeImportData(String sql) {
        try {
            // 解析导入命令: IMPORT [CSV|JSON] 'file_path' INTO table_name
            Pattern pattern = Pattern.compile("IMPORT\\s+(CSV|JSON)\\s+['\"](.+)['\"]\\s+INTO\\s+(\\w+)\\s*;?", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(sql);
            
            if (!matcher.find()) {
                return Message.createErrorResponse("client", "user", "导入语法错误: " + sql + 
                    ", 正确格式: IMPORT [CSV|JSON] 'file_path' INTO table_name");
            }
            
            String format = matcher.group(1).toUpperCase();
            String filePath = matcher.group(2);
            String tableName = matcher.group(3);
            
            // 检查文件是否存在
            File file = new File(filePath);
            if (!file.exists() || !file.isFile()) {
                return Message.createErrorResponse("client", "user", "文件不存在：" + filePath);
            }
            
            // 导入数据
            List<Map<String, Object>> dataList;
            if ("CSV".equals(format)) {
                dataList = importFromCsv(file);
            } else if ("JSON".equals(format)) {
                dataList = importFromJson(file);
            } else {
                return Message.createErrorResponse("client", "user", "不支持的导入格式: " + format);
            }
            
            // 获取表结构信息，验证数据是否符合要求
            List<Metadata.TableInfo> tables = getAllTables();
            Metadata.TableInfo tableInfo = null;
            
            for (Metadata.TableInfo table : tables) {
                if (table.getTableName().equals(tableName)) {
                    tableInfo = table;
                    break;
                }
            }
            
            if (tableInfo == null) {
                return Message.createErrorResponse("client", "user", "表不存在: " + tableName);
            }
            
            // 验证和插入数据
            int successCount = 0;
            List<String> errors = new ArrayList<>();
            
            for (Map<String, Object> data : dataList) {
                try {
                    Message result = insert(tableName, data);
                    if (result.getType() == Common.Message.MessageType.RESPONSE_SUCCESS) {
                        successCount++;
                    } else {
                        String errorMsg = result.getData("error") != null ? result.getData("error").toString() : "未知错误";
                        errors.add("导入行" + (successCount + errors.size() + 1) + "失败: " + errorMsg);
                    }
                } catch (Exception e) {
                    errors.add("导入行" + (successCount + errors.size() + 1) + "失败: " + e.getMessage());
                }
            }
            
            // 输出错误信息
            for (String error : errors) {
                System.out.println(error);
            }
            
            // 返回导入结果
            Message response = Message.createSuccessResponse("client", "user");
            response.setData("message", "导入数据完成，成功: " + successCount + "/" + dataList.size());
            if (!errors.isEmpty()) {
                response.setData("errors", errors);
            }
            
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "导入数据失败: " + e.getMessage());
        }
    }
    
    /**
     * 从CSV文件导入数据
     */
    private List<Map<String, Object>> importFromCsv(File file) throws IOException {
        List<Map<String, Object>> dataList = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // 读取表头
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return dataList; // 空文件
            }
            
            // 分割表头，获取列名
            String[] headers = headerLine.split(",");
            for (int i = 0; i < headers.length; i++) {
                headers[i] = headers[i].trim();
            }
            
            // 读取数据行
            String line;
            int lineNumber = 1; // 已读取表头
            
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                if (line.isEmpty()) {
                    continue; // 跳过空行
                }
                
                // 解析CSV行，处理引号等特殊情况
                String[] values = parseCSVLine(line);
                
                if (values.length != headers.length) {
                    System.out.println("警告: 行 " + lineNumber + " 的列数 (" + values.length + 
                        ") 与表头列数 (" + headers.length + ") 不匹配");
                    continue; // 跳过不匹配的行
                }
                
                // 构建数据记录
                Map<String, Object> record = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    String value = values[i].trim();
                    
                    // 尝试转换为适当的数据类型
                    Object parsedValue = parseValue(value);
                    record.put(headers[i], parsedValue);
                }
                
                dataList.add(record);
            }
        }
        
        return dataList;
    }
    
    /**
     * 解析CSV行，处理引号等特殊情况
     */
    private String[] parseCSVLine(String line) {
        List<String> values = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentValue = new StringBuilder();
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                // 处理引号
                if (inQuotes) {
                    // 检查是否是转义的引号 (""表示一个引号)
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        currentValue.append('"');
                        i++; // 跳过下一个引号
                    } else {
                        inQuotes = false;
                    }
                } else {
                    inQuotes = true;
                }
            } else if (c == ',' && !inQuotes) {
                // 找到字段分隔符，当前值结束
                values.add(currentValue.toString());
                currentValue = new StringBuilder();
            } else {
                // 普通字符，添加到当前值
                currentValue.append(c);
            }
        }
        
        // 添加最后一个值
        values.add(currentValue.toString());
        
        return values.toArray(new String[0]);
    }
    
    /**
     * 从JSON文件导入数据
     */
    private List<Map<String, Object>> importFromJson(File file) throws IOException {
        // 读取整个文件内容
        String content = new String(Files.readAllBytes(Paths.get(file.getPath())));
        
        // 简单解析JSON，假设文件格式为 [{...}, {...}, ...]
        List<Map<String, Object>> dataList = new ArrayList<>();
        
        // 去掉开头和结尾的方括号
        content = content.trim();
        if (content.startsWith("[")) {
            content = content.substring(1);
        }
        if (content.endsWith("]")) {
            content = content.substring(0, content.length() - 1);
        }
        
        // 分割JSON对象
        List<String> jsonObjects = splitJsonObjects(content);
        
        for (String jsonObject : jsonObjects) {
            Map<String, Object> record = parseJsonObject(jsonObject);
            if (!record.isEmpty()) {
                dataList.add(record);
            }
        }
        
        return dataList;
    }
    
    /**
     * 分割JSON对象字符串
     */
    private List<String> splitJsonObjects(String content) {
        List<String> objects = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int braceCount = 0;
        boolean inString = false;
        
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            
            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                // 处理字符串边界
                inString = !inString;
            }
            
            if (!inString) {
                if (c == '{') {
                    braceCount++;
                } else if (c == '}') {
                    braceCount--;
                } else if (c == ',' && braceCount == 0) {
                    // 对象之间的分隔符
                    if (current.length() > 0) {
                        objects.add(current.toString().trim());
                        current = new StringBuilder();
                    }
                    continue;
                }
            }
            
            current.append(c);
            
            // 完成一个对象
            if (braceCount == 0 && current.length() > 0 && current.charAt(0) == '{' && current.charAt(current.length() - 1) == '}') {
                objects.add(current.toString().trim());
                current = new StringBuilder();
            }
        }
        
        // 处理最后一个对象
        if (current.length() > 0) {
            objects.add(current.toString().trim());
        }
        
        return objects;
    }
    
    /**
     * 解析单个JSON对象
     */
    private Map<String, Object> parseJsonObject(String jsonObject) {
        Map<String, Object> result = new HashMap<>();
        
        // 去掉开头和结尾的花括号
        jsonObject = jsonObject.trim();
        if (jsonObject.startsWith("{")) {
            jsonObject = jsonObject.substring(1);
        }
        if (jsonObject.endsWith("}")) {
            jsonObject = jsonObject.substring(0, jsonObject.length() - 1);
        }
        
        // 分割键值对
        List<String> keyValuePairs = splitJsonKeyValuePairs(jsonObject);
        
        for (String pair : keyValuePairs) {
            String[] parts = pair.split(":", 2);
            if (parts.length == 2) {
                // 提取键（去掉引号）
                String key = parts[0].trim();
                if (key.startsWith("\"") && key.endsWith("\"")) {
                    key = key.substring(1, key.length() - 1);
                }
                
                // 提取值（去掉引号，如果是字符串）
                String valueStr = parts[1].trim();
                Object value;
                
                if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
                    // 字符串值
                    value = valueStr.substring(1, valueStr.length() - 1);
                } else if (valueStr.equals("null")) {
                    // null值
                    value = null;
                } else if (valueStr.equals("true") || valueStr.equals("false")) {
                    // 布尔值
                    value = Boolean.parseBoolean(valueStr);
                } else {
                    // 尝试解析为数字
                    try {
                        if (valueStr.contains(".")) {
                            value = Double.parseDouble(valueStr);
                        } else {
                            value = Integer.parseInt(valueStr);
                        }
                    } catch (NumberFormatException e) {
                        // 不是数字，保持原样
                        value = valueStr;
                    }
                }
                
                result.put(key, value);
            }
        }
        
        return result;
    }
    
    /**
     * 分割JSON对象中的键值对
     */
    private List<String> splitJsonKeyValuePairs(String content) {
        List<String> pairs = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        int braceCount = 0;
        int bracketCount = 0;
        
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            
            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                // 处理字符串边界
                inString = !inString;
            }
            
            if (!inString) {
                if (c == '{') {
                    braceCount++;
                } else if (c == '}') {
                    braceCount--;
                } else if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                } else if (c == ',' && braceCount == 0 && bracketCount == 0) {
                    // 键值对之间的分隔符
                    pairs.add(current.toString().trim());
                    current = new StringBuilder();
                    continue;
                }
            }
            
            current.append(c);
        }
        
        // 处理最后一个键值对
        if (current.length() > 0) {
            pairs.add(current.toString().trim());
        }
        
        return pairs;
    }
    
    /**
     * 执行DESCRIBE命令
     */
    private Message executeDescribe(String sql) throws RemoteException {
        // 解析DESCRIBE命令
        Pattern pattern = Pattern.compile("(DESCRIBE|DESC)\\s+(\\w+)\\s*;?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "语法错误: " + sql);
        }
        
        String tableName = matcher.group(2);
        
        // 获取表信息
        List<Metadata.TableInfo> tables = getAllTables();
        Metadata.TableInfo tableInfo = null;
        
        for (Metadata.TableInfo table : tables) {
            if (table.getTableName().equals(tableName)) {
                tableInfo = table;
                break;
            }
        }
        
        if (tableInfo == null) {
            return Message.createErrorResponse("client", "user", "表不存在: " + tableName);
        }
        
        // 打印表结构
        System.out.println("表名: " + tableInfo.getTableName());
        System.out.println("主键: " + tableInfo.getPrimaryKey());
        System.out.println();
        
        System.out.println("列名\t\t类型\t\t可空\t\t唯一");
        System.out.println("----------------------------------------");
        
        for (Metadata.ColumnInfo column : tableInfo.getColumns()) {
            System.out.println(column.getColumnName() + "\t\t" + 
                               column.getDataType() + "\t\t" + 
                               (!column.isNotNull() ? "是" : "否") + "\t\t" + 
                               (column.isUnique() ? "是" : "否"));
        }
        
        System.out.println();
        System.out.println("索引:");
        if (tableInfo.getIndexes().isEmpty()) {
            System.out.println("  (无)");
        } else {
            for (Metadata.IndexInfo index : tableInfo.getIndexes()) {
                System.out.println("  " + index.getIndexName() + " (" + index.getColumnName() + 
                                 (index.isUnique() ? ", 唯一" : "") + ")");
            }
        }
        
        return Message.createSuccessResponse("client", "user");
    }
    
    /**
     * 执行SHOW SHARDS命令
     */
    private Message executeShowShards(String sql) throws RemoteException {
        // 解析SHOW SHARDS命令
        Pattern pattern = Pattern.compile("SHOW\\s+SHARDS\\s+(\\w+)\\s*;?", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        
        // 获取表信息
        List<Metadata.TableInfo> tables = getAllTables();
        boolean tableExists = false;
        
        for (Metadata.TableInfo table : tables) {
            if (table.getTableName().equals(tableName)) {
                tableExists = true;
                break;
            }
        }
        
        if (!tableExists) {
            return Message.createErrorResponse("client", "user", "表不存在: " + tableName);
        }
        
        // 获取表的分片信息
        List<String> shards = masterService.getTableRegions(tableName);
        
        System.out.println("表 " + tableName + " 的分片情况:");
        System.out.println("----------------------------------------");
        
        if (shards.isEmpty()) {
            System.out.println("无分片信息");
        } else {
            int idx = 1;
            for (String server : shards) {
                System.out.println("分片 #" + idx + ": " + server);
                idx++;
            }
        }
        
        return Message.createSuccessResponse("client", "user");
    }
    
    /**
     * 执行SHOW SERVERS命令
     */
    private Message executeShowServers() throws RemoteException {
        try {
            // 获取所有RegionServer
            List<String> servers = getAllRegionServers();
            
            System.out.println("RegionServer状态:");
            System.out.println("----------------------------------------");
            
            if (servers.isEmpty()) {
                System.out.println("无RegionServer信息");
            } else {
                int idx = 1;
                for (String server : servers) {
                    // 获取RegionServer状态
                    RegionService regionService = RPCUtils.getRegionService(server);
                    Map<String, Object> status = new HashMap<>();
                    
                    try {
                        if (regionService != null) {
                            status = regionService.getStatus();
                        }
                    } catch (Exception e) {
                        System.err.println("获取RegionServer状态失败: " + e.getMessage());
                    }
                    
                    System.out.println("RegionServer #" + idx + ": " + server);
                    if (!status.isEmpty()) {
                        long uptime = status.containsKey("uptime") ? (long)status.get("uptime") : 0;
                        int tableCount = status.containsKey("tableCount") ? (int)status.get("tableCount") : 0;
                        System.out.println("  运行时间: " + formatUptime(uptime) + ", 表数量: " + tableCount);
                    } else {
                        System.out.println("  状态: 未知");
                    }
                    idx++;
                }
            }
            
            return Message.createSuccessResponse("client", "user");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "获取RegionServer信息失败: " + e.getMessage());
        }
    }
    
    /**
     * 格式化运行时间
     */
    private String formatUptime(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;
        
        if (days > 0) {
            return days + "天 " + (hours % 24) + "小时";
        } else if (hours > 0) {
            return hours + "小时 " + (minutes % 60) + "分钟";
        } else if (minutes > 0) {
            return minutes + "分钟 " + (seconds % 60) + "秒";
        } else {
            return seconds + "秒";
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
        try {
            // 尝试解析第一种格式：INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)
            Pattern pattern1 = Pattern.compile("INSERT\\s+INTO\\s+(\\w+)\\s*\\((.+?)\\)\\s*VALUES\\s*\\((.+?)\\);?", Pattern.CASE_INSENSITIVE);
            Matcher matcher1 = pattern1.matcher(sql);
            
            if (matcher1.find()) {
                String tableName = matcher1.group(1);
                String columnsStr = matcher1.group(2);
                String valuesStr = matcher1.group(3);
                
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
                    
                    // 转换为适当的数据类型
                    Object parsedValue = parseValue(value);
                    data.put(column, parsedValue);
                }
                
                // 调用RegionServer插入数据
                return insert(tableName, data);
            }
            
            // 尝试解析第二种格式：INSERT INTO table_name VALUES (val1, val2, ...)
            Pattern pattern2 = Pattern.compile("INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s*\\((.+?)\\);?", Pattern.CASE_INSENSITIVE);
            Matcher matcher2 = pattern2.matcher(sql);
            
            if (matcher2.find()) {
                String tableName = matcher2.group(1);
                String valuesStr = matcher2.group(2);
                
                // 获取表元数据
                List<Metadata.TableInfo> tables = getAllTables();
                Metadata.TableInfo tableInfo = null;
                
                for (Metadata.TableInfo table : tables) {
                    if (table.getTableName().equals(tableName)) {
                        tableInfo = table;
                        break;
                    }
                }
                
                if (tableInfo == null) {
                    return Message.createErrorResponse("client", "user", "表不存在: " + tableName);
                }
                
                // 解析值
                String[] values = valuesStr.split(",");
                List<Metadata.ColumnInfo> columns = tableInfo.getColumns();
                
                if (columns.size() != values.length) {
                    return Message.createErrorResponse("client", "user", "列数(" + columns.size() + ")与值数(" + values.length + ")不匹配");
                }
                
                // 构建插入数据
                Map<String, Object> data = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    String columnName = columns.get(i).getColumnName();
                    String value = values[i].trim();
                    
                    // 去掉引号
                    if (value.startsWith("'") && value.endsWith("'")) {
                        value = value.substring(1, value.length() - 1);
                    } else if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    
                    // 转换为适当的数据类型
                    Object parsedValue = parseValue(value);
                    data.put(columnName, parsedValue);
                }
                
                // 调用RegionServer插入数据
                return insert(tableName, data);
            }
            
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "插入数据失败: " + e.getMessage());
        }
    }
    
    /**
     * 执行DELETE语句
     */
    private Message executeDelete(String sql) throws RemoteException {
        // 去掉可能的分号
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        
        // 解析DELETE语句
        Pattern pattern = Pattern.compile("DELETE\\s+FROM\\s+(\\w+)(\\s+WHERE\\s+(.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        String whereClause = matcher.group(3); // 注意这里用group(3)，因为group(2)包含了WHERE关键字
        
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
                    
                    // 尝试转换为数值类型
                    Object parsedValue = parseValue(value);
                    conditions.put(column, parsedValue);
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
        Pattern pattern = Pattern.compile(
                "^SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+?))?;?\\s*$",
                Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            throw new RemoteException("SQL语法错误: " + sql);
        }
        
        String columnsStr = matcher.group(1);
        String tableName = matcher.group(2);
        String whereClause = matcher.group(3);
        
        // 解析列名
        List<String> columns = new ArrayList<>();
        boolean isCountQuery = false;
        
        // 检查是否是COUNT(*)查询
        if (columnsStr.toUpperCase().contains("COUNT(*)")) {
            isCountQuery = true;
            columns = new ArrayList<>(); // 空列表表示查询所有列
        } else if (!columnsStr.equals("*")) {
            String[] columnParts = columnsStr.split(",");
            for (String column : columnParts) {
                columns.add(column.trim());
            }
        }
        
        // 解析WHERE条件
        Map<String, Object> conditions = new HashMap<>();
        if (whereClause != null) {
            // 处理BETWEEN条件
            Pattern betweenPattern = Pattern.compile("(\\w+)\\s+BETWEEN\\s+(\\S+)\\s+AND\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
            Matcher betweenMatcher = betweenPattern.matcher(whereClause);
            
            if (betweenMatcher.find()) {
                String columnName = betweenMatcher.group(1).trim();
                String minValue = betweenMatcher.group(2).trim();
                String maxValue = betweenMatcher.group(3).trim();
                
                // 去掉引号
                if (minValue.startsWith("'") && minValue.endsWith("'")) {
                    minValue = minValue.substring(1, minValue.length() - 1);
                }
                if (maxValue.startsWith("'") && maxValue.endsWith("'")) {
                    maxValue = maxValue.substring(1, maxValue.length() - 1);
                }
                
                // 创建范围条件 (使用特殊格式标记为范围查询)
                Map<String, Object> rangeCondition = new HashMap<>();
                rangeCondition.put("min", parseValue(minValue));
                rangeCondition.put("max", parseValue(maxValue));
                conditions.put(columnName + "_range", rangeCondition);
            } else {
                // 处理普通条件
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
                        
                        // 尝试转换为数值类型
                        Object parsedValue = parseValue(value);
                        conditions.put(column, parsedValue);
                    }
                }
            }
        }
        
        // 调用RegionServer查询数据
        List<Map<String, Object>> result = select(tableName, columns, conditions);
        
        // 处理COUNT(*)查询
        if (isCountQuery) {
            Map<String, Object> countResult = new HashMap<>();
            countResult.put("COUNT(*)", result.size());
            result = Collections.singletonList(countResult);
        }
        
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
        // 去掉可能的分号
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        
        // 解析UPDATE语句 - 分两步：先提取表名和SET子句，再提取WHERE子句
        Pattern pattern = Pattern.compile("UPDATE\\s+(\\w+)\\s+SET\\s+(.+?)(\\s+WHERE\\s+(.+))?$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            return Message.createErrorResponse("client", "user", "SQL语法错误: " + sql);
        }
        
        String tableName = matcher.group(1);
        String setClause = matcher.group(2);
        String whereClause = matcher.group(4); // 注意这里用group(4)
        
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
                
                // 转换为适当的数据类型
                Object parsedValue = parseValue(value);
                values.put(column, parsedValue);
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
                    
                    // 尝试转换为数值类型
                    Object parsedValue = parseValue(value);
                    conditions.put(column, parsedValue);
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
            
            // 获取表的主键信息
            Metadata.TableInfo tableInfo = null;
            List<Metadata.TableInfo> tables = getAllTables();
            for (Metadata.TableInfo table : tables) {
                if (table.getTableName().equals(tableName)) {
                    tableInfo = table;
                    break;
                }
            }
            
            if (tableInfo == null) {
                return Message.createErrorResponse("client", "user", "表不存在: " + tableName);
            }
            
            // 获取表的主键
            String primaryKeyColumn = null;
            // 先获取表定义的主键
            primaryKeyColumn = tableInfo.getPrimaryKey();
            
            if (primaryKeyColumn == null) {
                return Message.createErrorResponse("client", "user", "表没有主键，无法确定分片位置");
            }
            
            // 获取主键值
            Object primaryKeyValue = values.get(primaryKeyColumn);
            if (primaryKeyValue == null) {
                return Message.createErrorResponse("client", "user", "主键值不能为空");
            }
            
            // 使用分片策略确定该数据应该存储在哪个RegionServer
            Common.ShardingStrategy shardingStrategy = new Common.HashShardingStrategy();
            String targetServer = shardingStrategy.getServerForKey(primaryKeyValue, servers);
            
            if (targetServer == null) {
                return Message.createErrorResponse("client", "user", "无法确定数据分片位置");
            }
            
            try {
                // 连接到目标RegionServer
                RegionService regionService = RPCUtils.getRegionService(targetServer);
                Message response = regionService.insert(tableName, values);
                
                // 如果成功插入到主分片，还需要插入备份
                if (response.getType() == Common.Message.MessageType.RESPONSE_SUCCESS) {
                    // 1. 插入到备份服务器（如果存在3个或更多RegionServer）
                    if (servers.size() >= 3) {
                        // 获取所有RegionServer列表
                        List<String> allServers = getAllRegionServers();
                        
                        if (allServers.size() >= 3) {
                            // 获取备份服务器（通常是按名称排序后的最后一个服务器）
                            List<String> backupServers = new ArrayList<>(allServers);
                            Collections.sort(backupServers);
                            String backupServer = backupServers.get(backupServers.size() - 1);
                            
                            // 如果备份服务器与目标服务器不同，则进行备份
                            if (!backupServer.equals(targetServer)) {
                                try {
                                    RegionService backupService = RPCUtils.getRegionService(backupServer);
                                    backupService.insert(tableName, values);
                                    System.out.println("数据已备份到服务器: " + backupServer);
                                } catch (Exception e) {
                                    System.err.println("备份数据到服务器 " + backupServer + " 失败: " + e.getMessage());
                                }
                            }
                        }
                    }
                    
                    return response;
                } else {
                    return response;
                }
            } catch (Exception e) {
                // 失败后尝试其他服务器
                System.err.println("RegionServer " + targetServer + " 插入失败: " + e.getMessage());
                RPCUtils.removeFromCache("region:" + targetServer);
                
                // 尝试任何其他可用的服务器
                Exception lastException = e;
                for (String server : servers) {
                    if (!server.equals(targetServer)) {
                        try {
                            RegionService regionService = RPCUtils.getRegionService(server);
                            return regionService.insert(tableName, values);
                        } catch (Exception ex) {
                            lastException = ex;
                            System.err.println("RegionServer " + server + " 插入失败: " + ex.getMessage());
                            RPCUtils.removeFromCache("region:" + server);
                        }
                    }
                }
                
                // 所有尝试都失败
                return Message.createErrorResponse("client", "user", "插入数据失败: " + lastException.getMessage());
            }
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
            
            // 尝试每个RegionServer，直到成功或全部失败
            List<String> unavailableServers = new ArrayList<>();
            Exception lastException = null;
            
            for (String regionServer : servers) {
                try {
                    RegionService regionService = RPCUtils.getRegionService(regionServer);
                    Message response = regionService.delete(tableName, conditions);
                    
                    // 删除成功，返回结果
                    return response;
                } catch (Exception e) {
                    unavailableServers.add(regionServer);
                    lastException = e;
                    System.err.println("RegionServer " + regionServer + " 删除失败: " + e.getMessage());
                    
                    // 从缓存中移除
                    RPCUtils.removeFromCache("region:" + regionServer);
                }
            }
            
            // 所有服务器都失败，尝试从Master更新表区域信息
            if (unavailableServers.size() == servers.size()) {
                try {
                    System.out.println("所有RegionServer不可用，尝试更新表区域信息");
                    updateTableRegions(tableName);
                    
                    // 获取更新后的服务器列表
                    List<String> updatedServers = tableRegions.get(tableName);
                    if (updatedServers != null && !updatedServers.isEmpty()) {
                        for (String server : updatedServers) {
                            if (!unavailableServers.contains(server)) {
                                try {
                                    RegionService regionService = RPCUtils.getRegionService(server);
                                    return regionService.delete(tableName, conditions);
                                } catch (Exception e) {
                                    System.err.println("更新后的RegionServer " + server + " 删除失败: " + e.getMessage());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("更新表区域信息失败: " + e.getMessage());
                }
            }
            
            // 如果到这里，说明所有尝试都失败了
            if (lastException != null) {
                return Message.createErrorResponse("client", "user", "删除数据失败: " + lastException.getMessage());
            }
            return Message.createErrorResponse("client", "user", "删除数据失败: 所有RegionServer不可用");
        } catch (Exception e) {
            e.printStackTrace();
            return Message.createErrorResponse("client", "user", "删除数据失败: " + e.getMessage());
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
            
            // 在所有RegionServer上执行更新，确保所有分片都得到更新
            Map<String, Exception> exceptions = new HashMap<>();
            Map<String, Integer> updateCounts = new HashMap<>();
            boolean hasSuccess = false;
            
            for (String regionServer : servers) {
                try {
                    RegionService regionService = RPCUtils.getRegionService(regionServer);
                    Message response = regionService.update(tableName, values, conditions);
                    
                    // 记录更新成功的服务器和更新的记录数
                    if (response.getType() == Common.Message.MessageType.RESPONSE_SUCCESS) {
                        hasSuccess = true;
                        int count = (Integer) response.getData("updatedCount");
                        updateCounts.put(regionServer, count);
                        if (count > 0) {
                            System.out.println("RegionServer " + regionServer + " 更新了 " + count + " 条记录");
                        }
                    } else {
                        String error = (String) response.getData("error");
                        exceptions.put(regionServer, new Exception(error));
                    }
                } catch (Exception e) {
                    exceptions.put(regionServer, e);
                    System.err.println("RegionServer " + regionServer + " 更新失败: " + e.getMessage());
                    
                    // 从缓存中移除
                    RPCUtils.removeFromCache("region:" + regionServer);
                }
            }
            
            // 根据更新结果返回消息
            if (hasSuccess) {
                // 计算总共更新的记录数
                int totalUpdated = updateCounts.values().stream().mapToInt(Integer::intValue).sum();
                
                // 即使部分服务器失败，只要有成功的就返回成功
                Message response = Message.createSuccessResponse("client", "user");
                response.setData("updatedCount", totalUpdated);
                
                // 如果有服务器失败，添加警告信息
                if (!exceptions.isEmpty()) {
                    StringBuilder warning = new StringBuilder("警告：部分RegionServer更新失败: ");
                    for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
                        warning.append(entry.getKey()).append("(").append(entry.getValue().getMessage()).append("); ");
                    }
                    response.setData("warning", warning.toString());
                }
                
                return response;
            } else {
                // 所有服务器都失败
                StringBuilder errorMsg = new StringBuilder("更新数据失败: ");
                for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
                    errorMsg.append(entry.getKey()).append("(").append(entry.getValue().getMessage()).append("); ");
                }
                return Message.createErrorResponse("client", "user", errorMsg.toString());
            }
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
     * 更新表区域信息
     */
    private void updateTableRegions(String tableName) throws RemoteException {
        List<String> regions = masterService.getTableRegions(tableName);
        tableRegions.put(tableName, regions);
    }
    
    /**
     * 将字符串转换为相应的数据类型
     */
    private Object parseValue(String value) {
        // 尝试转换为整数
        try {
            // 检查是否是整数格式
            if (value.matches("-?\\d+")) {
                Integer intValue = Integer.parseInt(value);
                return intValue;
            }
        } catch (NumberFormatException e) {
            // 无需打印异常信息
        }
        
        // 尝试转换为浮点数
        try {
            // 检查是否是浮点数格式 (允许 1.0, .5, 5., -1.5 等格式)
            if (value.matches("-?\\d*\\.\\d*") && !value.equals(".")) {
                Float floatValue = Float.parseFloat(value);
                return floatValue;
            }
        } catch (NumberFormatException e) {
            // 无需打印异常信息
        }
        
        // 检查布尔值
        if (value.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        } else if (value.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        
        // 无法转换，返回原始字符串
        return value;
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
            
            // 尝试每个RegionServer，直到成功或全部失败
            List<String> unavailableServers = new ArrayList<>();
            Exception lastException = null;
            List<Map<String, Object>> allResults = new ArrayList<>();
            
            for (String regionServer : servers) {
                try {
                    if (!masterService.getRegionServerStatus(regionServer)) {
                        unavailableServers.add(regionServer);
                        RPCUtils.removeFromCache("region:" + regionServer);
                    } else {
                        RegionService regionService = RPCUtils.getRegionService(regionServer);

                        // 调用RegionServer查询数据
                        List<Map<String, Object>> result = regionService.select(tableName, columns, conditions);
                        // 只有当结果不为空时才打印
                        if (result != null && !result.isEmpty()) {
                            System.out.println("成功从表 " + tableName + " 查询 " + result.size() + " 条记录");
                            // 将结果添加到总结果集
                            allResults.addAll(result);
                        }
                    }
                } catch (Exception e) {
                    unavailableServers.add(regionServer);
                    lastException = e;
                    System.err.println("RegionServer " + regionServer + " 查询失败: " + e.getMessage());
                    
                    // 从缓存中移除
                    RPCUtils.removeFromCache("region:" + regionServer);
                }
            }
            
            // 所有服务器都失败，尝试从Master更新表区域信息
            if (unavailableServers.size() == servers.size()) {
                try {
                    System.out.println("所有RegionServer不可用，尝试更新表区域信息");
                    updateTableRegions(tableName);
                    
                    // 获取更新后的服务器列表
                    List<String> updatedServers = tableRegions.get(tableName);
                    if (updatedServers != null && !updatedServers.isEmpty()) {
                        for (String server : updatedServers) {
                            if (!unavailableServers.contains(server)) {
                                try {
                                    RegionService regionService = RPCUtils.getRegionService(server);
                                    List<Map<String, Object>> result = regionService.select(tableName, columns, conditions);
                                    if (result != null && !result.isEmpty()) {
                                        allResults.addAll(result);
                                    }
                                } catch (Exception e) {
                                    System.err.println("更新后的RegionServer " + server + " 查询失败: " + e.getMessage());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("更新表区域信息失败: " + e.getMessage());
                }
            }
            
            // 对结果进行去重
            List<Map<String, Object>> uniqueResults = removeDuplicates(allResults);
            
            return uniqueResults;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("查询数据失败: " + e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * 移除结果集中的重复数据
     */
    private List<Map<String, Object>> removeDuplicates(List<Map<String, Object>> results) {
        if (results == null || results.isEmpty()) {
            return results;
        }
        
        // 使用LinkedHashSet保持顺序的同时去重
        Set<String> seenItems = new HashSet<>();
        List<Map<String, Object>> uniqueList = new ArrayList<>();
        
        for (Map<String, Object> row : results) {
            // 将行转换为唯一的字符串表示
            String rowKey = generateRowKey(row);
            if (!seenItems.contains(rowKey)) {
                seenItems.add(rowKey);
                uniqueList.add(row);
            }
        }
        
        return uniqueList;
    }
    
    /**
     * 为行数据生成唯一键
     */
    private String generateRowKey(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder();
        // 按键排序，确保相同数据生成相同的键
        List<String> keys = new ArrayList<>(row.keySet());
        Collections.sort(keys);
        
        for (String key : keys) {
            Object value = row.get(key);
            sb.append(key).append("=");
            sb.append(value == null ? "null" : value.toString());
            sb.append(";");
        }
        
        return sb.toString();
    }

    /**
     * 执行SELECT FROM SHARD语句，从指定分片查询数据
     */
    private void executeSelectFromShard(String sql) throws RemoteException {
        // 解析SELECT FROM SHARD语句
        Pattern pattern = Pattern.compile("SELECT\\s+FROM\\s+SHARD\\s+(\\S+)\\s+(.+?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+?))?;?", 
                                       Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        
        if (!matcher.find()) {
            throw new RemoteException("SQL语法错误: " + sql + "\n正确语法: SELECT FROM SHARD <server:port> <columns> FROM <table> [WHERE <conditions>]");
        }
        
        String shardServer = matcher.group(1);
        String columnsStr = matcher.group(2);
        String tableName = matcher.group(3);
        String whereClause = matcher.group(4);
        
        // 解析列名
        List<String> columns = new ArrayList<>();
        boolean isCountQuery = false;
        
        // 检查是否是COUNT(*)查询
        if (columnsStr.toUpperCase().contains("COUNT(*)")) {
            isCountQuery = true;
            columns = new ArrayList<>(); // 空列表表示查询所有列
        } else if (!columnsStr.equals("*")) {
            String[] columnParts = columnsStr.split(",");
            for (String column : columnParts) {
                columns.add(column.trim());
            }
        }
        
        // 解析WHERE条件
        Map<String, Object> conditions = new HashMap<>();
        if (whereClause != null) {
            // 处理BETWEEN条件
            Pattern betweenPattern = Pattern.compile("(\\w+)\\s+BETWEEN\\s+(\\S+)\\s+AND\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
            Matcher betweenMatcher = betweenPattern.matcher(whereClause);
            
            if (betweenMatcher.find()) {
                String columnName = betweenMatcher.group(1).trim();
                String minValue = betweenMatcher.group(2).trim();
                String maxValue = betweenMatcher.group(3).trim();
                
                // 去掉引号
                if (minValue.startsWith("'") && minValue.endsWith("'")) {
                    minValue = minValue.substring(1, minValue.length() - 1);
                }
                if (maxValue.startsWith("'") && maxValue.endsWith("'")) {
                    maxValue = maxValue.substring(1, maxValue.length() - 1);
                }
                
                // 创建范围条件 (使用特殊格式标记为范围查询)
                Map<String, Object> rangeCondition = new HashMap<>();
                rangeCondition.put("min", parseValue(minValue));
                rangeCondition.put("max", parseValue(maxValue));
                conditions.put(columnName + "_range", rangeCondition);
            } else {
                // 处理普通条件
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
                        
                        // 尝试转换为数值类型
                        Object parsedValue = parseValue(value);
                        conditions.put(column, parsedValue);
                    }
                }
            }
        }
        
        try {
            // 直接连接到指定的RegionServer
            RegionService regionService = RPCUtils.getRegionService(shardServer);
            
            if (regionService == null) {
                System.out.println("无法连接到指定的RegionServer: " + shardServer);
                return;
            }
            
            // 调用RegionServer查询数据
            List<Map<String, Object>> result = regionService.select(tableName, columns, conditions);
            
            // 处理COUNT(*)查询
            if (isCountQuery) {
                Map<String, Object> countResult = new HashMap<>();
                countResult.put("COUNT(*)", result.size());
                result = Collections.singletonList(countResult);
            }
            
            // 打印结果
            System.out.println("从分片 " + shardServer + " 查询结果:");
            printResult(result);
        } catch (Exception e) {
            System.err.println("从分片 " + shardServer + " 查询失败: " + e.getMessage());
            throw new RemoteException("查询失败: " + e.getMessage());
        }
    }
} 