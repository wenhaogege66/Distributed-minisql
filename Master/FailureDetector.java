package Master;

import Common.Message;
import Common.Metadata;
import Common.RPCUtils;
import Common.ZKUtils;
import RegionServer.RegionService;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RegionServer故障检测和恢复
 */
public class FailureDetector implements Watcher {
    
    private ZKUtils zkUtils;
    private MasterServiceImpl masterService;
    
    // 保存所有RegionServer的状态
    private Map<String, String> regionServers;
    
    // 保存Region表映射
    private Map<String, List<String>> regionTables;
    
    // 恢复任务执行器
    private ScheduledExecutorService recoveryExecutor;
    
    /**
     * 构造函数
     */
    public FailureDetector(MasterServiceImpl masterService, ZKUtils zkUtils, Map<String, String> regionServers) {
        this.masterService = masterService;
        this.zkUtils = zkUtils;
        this.regionServers = regionServers;
        this.regionTables = new ConcurrentHashMap<>();
        
        // 创建恢复任务执行器
        this.recoveryExecutor = Executors.newScheduledThreadPool(1);
        
        // 定期检查RegionServer状态
        startHealthCheck();
    }
    
    /**
     * 开始健康检查
     */
    private void startHealthCheck() {
        recoveryExecutor.scheduleAtFixedRate(() -> {
            try {
                checkRegionServersHealth();
            } catch (Exception e) {
                System.err.println("健康检查异常: " + e.getMessage());
                e.printStackTrace();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
    
    /**
     * 检查RegionServer健康状态
     */
    private void checkRegionServersHealth() {
        // 获取当前所有RegionServer
        Set<String> currentServers = new HashSet<>(regionServers.keySet());
        
        for (String server : currentServers) {
            try {
                // 尝试连接RegionServer
                RegionService regionService = RPCUtils.getRegionService(server);
                if (regionService == null || !regionService.heartbeat()) {
                    // 连接失败或心跳检测失败，将RegionServer标记为故障
                    handleRegionServerFailure(server);
                }
            } catch (Exception e) {
                // 连接异常，将RegionServer标记为故障
                handleRegionServerFailure(server);
            }
        }
    }
    
    /**
     * 处理RegionServer故障
     */
    private void handleRegionServerFailure(String failedServer) {
        System.out.println("检测到RegionServer故障: " + failedServer);
        
        // 修改RegionServer状态
        regionServers.put(failedServer, "failed");
        
        // 查找故障RegionServer上的所有表
        List<String> affectedTables = new ArrayList<>();
        for (Map.Entry<String, Metadata.TableRegionInfo> entry : masterService.getTableRegions().entrySet()) {
            String tableName = entry.getKey();
            Metadata.TableRegionInfo regionInfo = entry.getValue();
            
            if (regionInfo.getRegionServers().contains(failedServer)) {
                affectedTables.add(tableName);
            }
        }
        
        if (!affectedTables.isEmpty()) {
            System.out.println("开始恢复故障RegionServer: " + failedServer + " 上的表: " + affectedTables);
            
            // 启动恢复过程
            recoverTables(failedServer, affectedTables);
        }
    }
    
    /**
     * 恢复故障RegionServer上的表
     */
    private void recoverTables(String failedServer, List<String> affectedTables) {
        // 获取可用的RegionServer列表
        List<String> availableServers = new ArrayList<>();
        for (Map.Entry<String, String> entry : regionServers.entrySet()) {
            if (!"failed".equals(entry.getValue()) && !entry.getKey().equals(failedServer)) {
                availableServers.add(entry.getKey());
            }
        }
        
        if (availableServers.isEmpty()) {
            System.err.println("没有可用的RegionServer进行故障恢复");
            return;
        }
        
        // 计算每个RegionServer的负载
        Map<String, Integer> serverLoads = masterService.calculateServerLoads();
        
        // 对每个受影响的表进行恢复
        for (String tableName : affectedTables) {
            try {
                recoverTable(tableName, failedServer, availableServers, serverLoads);
            } catch (Exception e) {
                System.err.println("恢复表 " + tableName + " 失败: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 恢复单个表
     */
    private void recoverTable(String tableName, String failedServer, 
                             List<String> availableServers, Map<String, Integer> serverLoads) {
        try {
            // 获取表信息
            Metadata.TableInfo tableInfo = masterService.getTableInfo(tableName);
            if (tableInfo == null) {
                System.err.println("找不到表 " + tableName + " 的元数据");
                return;
            }
            
            Metadata.TableRegionInfo regionInfo = masterService.getTableRegionInfo(tableName);
            if (regionInfo == null) {
                System.err.println("找不到表 " + tableName + " 的区域信息");
                return;
            }
            
            // 从表区域信息中移除故障的RegionServer
            regionInfo.removeRegionServer(failedServer);
            
            // 识别备份服务器 - 按名称排序后的最后一个服务器
            List<String> allServers = masterService.getAllRegionServers();
            String backupServer = null;
            
            if (allServers.size() >= 3) {
                List<String> sortedServers = new ArrayList<>(allServers);
                Collections.sort(sortedServers);
                backupServer = sortedServers.get(sortedServers.size() - 1);
            }
            
            // 根据负载选择新的RegionServer
            String newServer = null;
            
            // 检查当前的RegionServer数量是否足够维持表的副本
            List<String> currentServers = regionInfo.getRegionServers();
            int requiredReplicaCount = 2; // 假设我们需要的最小副本数
            
            // 如果当前服务器数量已经满足要求，可能不需要添加新的服务器
            if (currentServers.size() >= requiredReplicaCount) {
                System.out.println("表 " + tableName + " 的剩余副本数量足够 (" + currentServers.size() + 
                                  "/" + requiredReplicaCount + ")，不需要添加新服务器");
                
                // 更新ZooKeeper中的数据
                masterService.updateTableRegionInfo(tableName, regionInfo);
                return;
            }
            
            // 需要选择新的RegionServer
            if (backupServer != null && availableServers.contains(backupServer) && 
                !regionInfo.getRegionServers().contains(backupServer)) {
                // 优先选择备份服务器
                System.out.println("检测到可用的备份服务器: " + backupServer);
                newServer = backupServer;
            } else {
                // 从剩余服务器中选择负载最低的
                newServer = selectLeastLoadedServer(availableServers, serverLoads, 
                                                   regionInfo.getRegionServers());
            }
            
            if (newServer == null) {
                System.err.println("无法找到合适的RegionServer恢复表 " + tableName);
                return;
            }
            
            // 确定源服务器（数据从哪里恢复）
            String sourceServer = null;
            
            // 从当前存活的RegionServer中选择一个作为数据源
            for (String server : currentServers) {
                if (availableServers.contains(server)) {
                    sourceServer = server;
                    break;
                }
            }
            
            // 如果没有找到合适的源服务器，尝试使用备份服务器
            if (sourceServer == null && backupServer != null && availableServers.contains(backupServer)) {
                sourceServer = backupServer;
            }
            
            if (sourceServer == null) {
                System.err.println("找不到表 " + tableName + " 的可用数据源");
                return;
            }
            
            // 如果源RegionServer和新RegionServer相同，则无需复制
            if (sourceServer.equals(newServer)) {
                // 更新表区域信息（只更新ZooKeeper中的元数据，不复制数据）
                System.out.println("源服务器 " + sourceServer + " 和目标服务器相同，无需复制数据");
                masterService.updateTableRegionInfo(tableName, regionInfo);
                return;
            }
            
            System.out.println("开始将表 " + tableName + " 从 " + sourceServer + " 恢复到 " + newServer);
            
            // 创建表并复制数据
            boolean success = false;
            
            try {
                // 在目标RegionServer创建表
                RegionService targetRegionService = RPCUtils.getRegionService(newServer);
                Message createResponse = targetRegionService.createTable(tableInfo);
                
                if (createResponse.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                    System.err.println("在 " + newServer + " 上创建表失败: " + createResponse.getData("error"));
                    return;
                }
                
                // 获取表的主键信息，用于确定分片
                String primaryKeyColumn = tableInfo.getPrimaryKey();
                if (primaryKeyColumn == null) {
                    System.err.println("表 " + tableName + " 没有主键，无法确定分片");
                    return;
                }
                
                // 从源RegionServer获取数据
                RegionService sourceRegionService = RPCUtils.getRegionService(sourceServer);
                List<Map<String, Object>> allData = sourceRegionService.getAllTableData(tableName);
                
                if (allData == null || allData.isEmpty()) {
                    System.out.println("源服务器 " + sourceServer + " 没有表 " + tableName + " 的数据");
                    // 更新表区域信息
                    regionInfo.addRegionServer(newServer);
                    masterService.updateTableRegionInfo(tableName, regionInfo);
                    return;
                }
                
                // 获取当前可用的所有服务器（包括新服务器）
                List<String> allAvailableServers = new ArrayList<>(availableServers);
                allAvailableServers.add(newServer);
                
                // 使用备份服务器时，复制所有数据（备份服务器总是保存完整数据）
                if (sourceServer.equals(backupServer) || newServer.equals(backupServer)) {
                    System.out.println("使用备份服务器，将复制所有数据");
                    
                    // 复制所有数据
                    int successCount = 0;
                    for (Map<String, Object> record : allData) {
                        try {
                            Message insertResponse = targetRegionService.insert(tableName, record);
                            if (insertResponse.getType() == Common.Message.MessageType.RESPONSE_SUCCESS) {
                                successCount++;
                            }
                        } catch (Exception e) {
                            System.err.println("插入数据失败: " + e.getMessage());
                        }
                    }
                    
                    System.out.println("复制了 " + successCount + "/" + allData.size() + " 条记录到 " + newServer);
                    success = true;
                } else {
                    // 分片复制：只复制应该属于新节点的数据
                    Common.ShardingStrategy shardingStrategy = new Common.HashShardingStrategy();
                    
                    // 获取剩余可用的RegionServer（用于数据分片计算）
                    // 移除备份服务器（如果有），因为它不参与常规数据分片
                    List<String> shardingServers = new ArrayList<>(allAvailableServers);
                    if (backupServer != null) {
                        shardingServers.remove(backupServer);
                    }
                    
                    int successCount = 0;
                    int totalToCopy = 0;
                    
                    // 遍历所有数据，复制应该存储在新RegionServer上的数据
                    for (Map<String, Object> record : allData) {
                        Object primaryKeyValue = record.get(primaryKeyColumn);
                        if (primaryKeyValue == null) {
                            continue;
                        }
                        
                        // 计算该记录应该存储的服务器
                        String targetServer = shardingStrategy.getServerForKey(primaryKeyValue, shardingServers);
                        
                        // 如果应该存储在新服务器上，则复制
                        if (newServer.equals(targetServer)) {
                            totalToCopy++;
                            try {
                                Message insertResponse = targetRegionService.insert(tableName, record);
                                if (insertResponse.getType() == Common.Message.MessageType.RESPONSE_SUCCESS) {
                                    successCount++;
                                }
                            } catch (Exception e) {
                                System.err.println("插入数据失败: " + e.getMessage());
                            }
                        }
                    }
                    
                    System.out.println("根据分片策略，复制了 " + successCount + "/" + totalToCopy + 
                                     " 条记录到 " + newServer + "（总数据量: " + allData.size() + "）");
                    success = (successCount > 0 || totalToCopy == 0);
                }
                
                if (success) {
                    // 更新表区域信息
                    regionInfo.addRegionServer(newServer);
                    masterService.updateTableRegionInfo(tableName, regionInfo);
                    
                    // 更新serverLoads
                    serverLoads.put(newServer, serverLoads.getOrDefault(newServer, 0) + 1);
                    
                    System.out.println("成功恢复表 " + tableName + " 从 " + sourceServer + " 到 " + newServer);
                }
            } catch (Exception e) {
                System.err.println("复制表数据异常: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println("恢复表 " + tableName + " 时出现异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 选择负载最低的RegionServer
     */
    private String selectLeastLoadedServer(List<String> availableServers, 
                                          Map<String, Integer> serverLoads,
                                          List<String> excludeServers) {
        if (availableServers.isEmpty()) {
            return null;
        }
        
        // 复制可用服务器列表
        List<String> servers = new ArrayList<>(availableServers);
        
        // 移除已被排除的服务器
        if (excludeServers != null) {
            servers.removeAll(excludeServers);
        }
        
        if (servers.isEmpty()) {
            return null;
        }
        
        // 找出负载最低的服务器
        String leastLoadedServer = servers.get(0);
        int minLoad = serverLoads.getOrDefault(leastLoadedServer, 0);
        
        for (String server : servers) {
            int load = serverLoads.getOrDefault(server, 0);
            if (load < minLoad) {
                minLoad = load;
                leastLoadedServer = server;
            }
        }
        
        return leastLoadedServer;
    }
    
    @Override
    public void process(WatchedEvent event) {
        try {
            // 处理RegionServer节点变化事件
            if (event.getPath() != null && event.getPath().startsWith(ZKUtils.REGION_SERVERS_NODE)) {
                // 重新获取所有RegionServer状态
                masterService.watchRegionServers();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 关闭故障检测器
     */
    public void shutdown() {
        if (recoveryExecutor != null) {
            recoveryExecutor.shutdown();
            try {
                if (!recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    recoveryExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                recoveryExecutor.shutdownNow();
            }
        }
    }
} 