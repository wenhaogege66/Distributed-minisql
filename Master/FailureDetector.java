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
            
            if (backupServer != null && availableServers.contains(backupServer) && 
                !regionInfo.getRegionServers().contains(backupServer)) {
                // 如果备份服务器可用且不在当前表的RegionServer列表中，则可能是备份服务器可以作为源
                System.out.println("检测到可用的备份服务器: " + backupServer);
                newServer = selectLeastLoadedServer(availableServers, serverLoads, backupServer);
            } else {
                // 常规选择最低负载的服务器
                newServer = selectLeastLoadedServer(availableServers, serverLoads, null);
            }
            
            if (newServer == null) {
                System.err.println("无法找到合适的RegionServer恢复表 " + tableName);
                return;
            }
            
            // 确定源服务器（数据从哪里恢复）
            String sourceServer = null;
            
            // 优先使用备份服务器作为数据源
            if (backupServer != null && availableServers.contains(backupServer)) {
                // 检查备份服务器是否有这个表
                try {
                    RegionService backupService = RPCUtils.getRegionService(backupServer);
                    if (backupService != null) {
                        List<Map<String, Object>> data = backupService.getAllTableData(tableName);
                        if (data != null && !data.isEmpty()) {
                            sourceServer = backupServer;
                            System.out.println("从备份服务器 " + backupServer + " 恢复表 " + tableName);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("检查备份服务器 " + backupServer + " 数据时出错: " + e.getMessage());
                }
            }
            
            // 如果备份服务器不可用或没有数据，尝试从其他正常的RegionServer恢复
            if (sourceServer == null) {
                List<String> currentServers = regionInfo.getRegionServers();
                for (String server : currentServers) {
                    if (!server.equals(failedServer)) {
                        sourceServer = server;
                        break;
                    }
                }
            }
            
            if (sourceServer == null) {
                System.err.println("找不到表 " + tableName + " 的可用数据源");
                return;
            }
            
            // 如果源RegionServer和新RegionServer相同，则无需复制
            if (!sourceServer.equals(newServer)) {
                // 从源RegionServer复制表数据到新RegionServer
                boolean success = replicateTable(tableInfo, sourceServer, newServer);

                if (success) {
                    // 更新表区域信息
                    regionInfo.addRegionServer(newServer);

                    // 更新ZooKeeper中的数据
                    masterService.updateTableRegionInfo(tableName, regionInfo);

                    // 更新serverLoads
                    serverLoads.put(newServer, serverLoads.getOrDefault(newServer, 0) + 1);

                    System.out.println("成功恢复表 " + tableName + " 从 " + sourceServer + " 到 " + newServer);
                } else {
                    System.err.println("复制表 " + tableName + " 从 " + sourceServer + " 到 " + newServer + " 失败");
                }
            } else {
                // 更新ZooKeeper中的数据
                masterService.updateTableRegionInfo(tableName, regionInfo);
                System.out.println(sourceServer + "已包含表" + tableName + "的副本");
            }

        } catch (Exception e) {
            System.err.println("恢复表 " + tableName + " 时出现异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 复制表数据
     */
    private boolean replicateTable(Metadata.TableInfo tableInfo, String sourceServer, String targetServer) {
        try {
            // 在目标RegionServer上创建表
            RegionService targetRegionService = RPCUtils.getRegionService(targetServer);
            Message createResponse = targetRegionService.createTable(tableInfo);
            
            if (createResponse.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                System.err.println("创建表失败: " + createResponse.getData("error"));
                return false;
            }
            
            // 从源RegionServer获取数据
            RegionService sourceRegionService = RPCUtils.getRegionService(sourceServer);
            List<Map<String, Object>> data = sourceRegionService.getAllTableData(tableInfo.getTableName());
            
            // 将数据插入到目标RegionServer
            for (Map<String, Object> record : data) {
                Message insertResponse = targetRegionService.insert(tableInfo.getTableName(), record);
                if (insertResponse.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                    System.err.println("插入数据失败: " + insertResponse.getData("error"));
                    // 继续复制其他记录
                }
            }
            
            return true;
        } catch (Exception e) {
            System.err.println("复制表数据异常: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 选择负载最低的RegionServer
     */
    private String selectLeastLoadedServer(List<String> availableServers, 
                                          Map<String, Integer> serverLoads,
                                          String excludeServer) {
        if (availableServers.isEmpty()) {
            return null;
        }
        
        List<String> servers = new ArrayList<>(availableServers);
        if (excludeServer != null) {
            servers.remove(excludeServer);
            if (servers.isEmpty()) {
                return null;
            }
        }
        
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