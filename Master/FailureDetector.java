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
            Common.HashShardingStrategy shardingStrategy = new Common.HashShardingStrategy();
            String backupServer = shardingStrategy.getBackupServer(allServers);
            
            // 确定目标服务器，从可用服务器中选择负载最低的
            String targetServer = null;
            
            // 如果失败的服务器是备份服务器，则需要重新指定一个备份服务器
            if (failedServer.equals(backupServer)) {
                // 需要从正常的分片服务器中重新构建一个备份服务器
                // 按名称排序可用服务器，选择新的备份服务器
                List<String> sortedAvailableServers = new ArrayList<>(availableServers);
                Collections.sort(sortedAvailableServers);
                String newBackupServer = sortedAvailableServers.get(sortedAvailableServers.size() - 1);
                
                // 将新备份服务器添加到恢复目标
                if (!regionInfo.getRegionServers().contains(newBackupServer)) {
                    regionInfo.addRegionServer(newBackupServer);
                    
                    // 需要从所有其他服务器收集数据到新的备份服务器
                    for (String server : regionInfo.getRegionServers()) {
                        if (!server.equals(newBackupServer)) {
                            // 从现有服务器复制全部数据到新备份服务器
                            try {
                                RegionService sourceService = RPCUtils.getRegionService(server);
                                RegionService targetService = RPCUtils.getRegionService(newBackupServer);
                                
                                // 获取源服务器上的所有数据
                                List<Map<String, Object>> allData = sourceService.select(tableName, null, new HashMap<>());
                                
                                // 插入到新备份服务器
                                for (Map<String, Object> record : allData) {
                                    targetService.insert(tableName, record);
                                }
                                
                                System.out.println("表 " + tableName + " 的数据已从 " + server + 
                                                 " 复制到新的备份服务器 " + newBackupServer);
                            } catch (Exception e) {
                                System.err.println("从 " + server + " 复制数据到 " + newBackupServer + " 失败: " + e.getMessage());
                            }
                        }
                    }
                }
            } else {
                // 如果失败的是普通分片服务器
                // 1. 检查是否需要添加新的服务器
                boolean needNewServer = regionInfo.getRegionServers().size() < 2; // 至少需要2个服务器
                
                if (needNewServer && availableServers.size() > regionInfo.getRegionServers().size()) {
                    // 按负载排序可用服务器
                    List<String> candidateServers = new ArrayList<>(availableServers);
                    candidateServers.removeAll(regionInfo.getRegionServers()); // 移除已经在使用的服务器
                    
                    // 如果备份服务器可用，从候选服务器中移除它，因为它不应该用作普通分片服务器
                    if (backupServer != null) {
                        candidateServers.remove(backupServer);
                    }
                    
                    if (!candidateServers.isEmpty()) {
                        // 按负载排序候选服务器
                        candidateServers.sort((s1, s2) -> serverLoads.getOrDefault(s1, 0) - serverLoads.getOrDefault(s2, 0));
                        targetServer = candidateServers.get(0); // 选择负载最低的
                        
                        if (targetServer != null) {
                            regionInfo.addRegionServer(targetServer);
                            
                            // 如果已经有备份服务器，从其中获取失败节点的数据
                            if (backupServer != null && !backupServer.equals(failedServer) && 
                                !regionInfo.getRegionServers().contains(backupServer)) {
                                try {
                                    RegionService backupService = RPCUtils.getRegionService(backupServer);
                                    RegionService targetService = RPCUtils.getRegionService(targetServer);
                                    
                                    // 从备份服务器获取数据
                                    List<Map<String, Object>> allData = backupService.select(tableName, null, new HashMap<>());
                                    
                                    // 根据分片策略过滤应该存储在目标服务器上的数据
                                    List<String> shardingServers = shardingStrategy.getShardingServers(regionInfo.getRegionServers());
                                    String primaryKeyColumn = tableInfo.getPrimaryKey();
                                    
                                    for (Map<String, Object> record : allData) {
                                        Object primaryKeyValue = record.get(primaryKeyColumn);
                                        if (primaryKeyValue != null) {
                                            // 确定该记录应该存储在哪个分片服务器上
                                            String assignedServer = shardingStrategy.getServerForKey(primaryKeyValue, shardingServers);
                                            
                                            // 如果应该存储在目标服务器上，则插入
                                            if (targetServer.equals(assignedServer)) {
                                                targetService.insert(tableName, record);
                                            }
                                        }
                                    }
                                    
                                    System.out.println("表 " + tableName + " 的数据已从备份服务器 " + backupServer + 
                                                     " 恢复到 " + targetServer);
                                } catch (Exception e) {
                                    System.err.println("从备份服务器 " + backupServer + " 恢复数据到 " + targetServer + 
                                                     " 失败: " + e.getMessage());
                                }
                            }
                        }
                    }
                }
            }
            
            // 更新表的区域信息
            masterService.updateTableRegionInfo(tableName, regionInfo);
        } catch (Exception e) {
            System.err.println("恢复表 " + tableName + " 失败: " + e.getMessage());
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