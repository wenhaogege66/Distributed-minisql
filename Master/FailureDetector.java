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
            //List<String> allServers = masterService.getAllRegionServers();
            Common.HashShardingStrategy shardingStrategy = new Common.HashShardingStrategy();
            //String backupServer = shardingStrategy.getBackupServer(allServers);


            //新构建的根据表名获取备份服务器的方法
            String backupServer = masterService.getBackupServer(tableName);


            
            System.out.println("处理表 " + tableName + " 的恢复，故障服务器: " + failedServer);
            System.out.println("备份服务器: " + backupServer);
            System.out.println("当前可用服务器: " + availableServers);
            
            // 如果失败的服务器是备份服务器，则需要重新指定一个备份服务器
            if (failedServer.equals(backupServer)) {
                System.out.println("备份服务器 " + backupServer + " 发生故障，需要重新指定新的备份服务器");

                //找寻不存在该表的空闲服务器充当新的备份服务器
                List<String> shardServers = regionInfo.getRegionServers();
                availableServers.removeAll(shardServers);


                //存在空闲服务器
                if (!availableServers.isEmpty()) {

                    //选择其中之一作为备份服务器
                    List<String> sortedAvailableServers = new ArrayList<>(availableServers);
                    Collections.sort(sortedAvailableServers);
                    String newBackupServer = sortedAvailableServers.get(sortedAvailableServers.size() - 1);
                    System.out.println("选择 " + newBackupServer + " 作为新的备份服务器");
                    
                    // 检查新备份服务器是否已在区域信息中
                    if (!regionInfo.getRegionServers().contains(newBackupServer)) {
                        regionInfo.addRegionServer(newBackupServer);
                    }
                    
                    // 从所有其他可用的RegionServer收集数据至新的备份服务器
                    System.out.println("开始从其他分片服务器收集数据到新备份服务器");
                    //在新的备份服务器上创建表
                    Metadata.TableInfo tableinfo = masterService.getTableInfo(tableName);
                    RegionService targetRegion = RPCUtils.getRegionService(newBackupServer);
                    targetRegion.createTable(tableInfo);

                    for (String server : regionInfo.getRegionServers()) {
                        if (!server.equals(newBackupServer)) {
                            try {
                                RegionService sourceRegion = RPCUtils.getRegionService(server);



                                if (sourceRegion != null && targetRegion != null) {
                                    // 获取源服务器上的所有数据
                                    List<Map<String, Object>> allData = sourceRegion.select(tableName, null, new HashMap<>());
                                    System.out.println("从 " + server + " 获取到 " + allData.size() + " 条数据");
                                    
                                    // 复制到新备份服务器
                                    for (Map<String, Object> row : allData) {
                                        targetRegion.insert(tableName, row);
                                    }
                                    System.out.println("成功将数据从 " + server + " 复制到新备份服务器 " + newBackupServer);
                                }
                            } catch (Exception e) {
                                System.err.println("从 " + server + " 复制数据到 " + newBackupServer + " 失败: " + e.getMessage());
                            }
                        }
                    }
                    //数据转移完成，更改表的备份信息
                    masterService.setBackupServer(tableName,newBackupServer);

                } else {
                    //不存在空闲服务器
                    System.err.println("没有空闲服务器可以指定为新的备份服务器");
                }
            } else {
                // 如果失败的是普通分片服务器
                System.out.println("分片服务器 " + failedServer + " 发生故障，需要恢复其负责的分片数据");
                
                // 检查是否需要添加新的分片服务器
                boolean needNewServer = regionInfo.getRegionServers().size() < 2; // 确保至少有2个服务器（包括备份）
                
                if (needNewServer) {
                    // 从可用服务器中选择一个新的分片服务器
                    List<String> candidateServers = new ArrayList<>(availableServers);
                    // 排除已经在使用的服务器
                    candidateServers.removeAll(regionInfo.getRegionServers());
                    // 排除备份服务器(如果备份服务器在可用列表中)
                    if (backupServer != null) {
                        candidateServers.remove(backupServer);
                    }
                    
                    // 按负载排序候选服务器，选择负载最低的
                    if (!candidateServers.isEmpty()) {
                        candidateServers.sort((s1, s2) -> 
                            serverLoads.getOrDefault(s1, 0) - serverLoads.getOrDefault(s2, 0));
                        
                        String newServer = candidateServers.get(0);
                        System.out.println("选择 " + newServer + " 作为新的分片服务器，替代故障服务器 " + failedServer);
                        
                        // 添加新服务器到区域信息
                        regionInfo.addRegionServer(newServer);
                        // 在新的分片服务器上创建表
                        Metadata.TableInfo tableinfo = masterService.getTableInfo(tableName);
                        RegionService targetRegion = RPCUtils.getRegionService(newServer);
                        targetRegion.createTable(tableInfo);
                        // 从备份服务器恢复数据至新服务器
                        if (backupServer != null && !backupServer.equals(failedServer)) {
                            try {
                                RegionService backupRegion = RPCUtils.getRegionService(backupServer);
                                RegionService newRegion = RPCUtils.getRegionService(newServer);
                                
                                if (backupRegion != null && newRegion != null) {
                                    // 获取备份服务器上的所有数据
                                    List<Map<String, Object>> allData = backupRegion.select(tableName, null, new HashMap<>());
                                    System.out.println("从备份服务器 " + backupServer + " 获取到 " + allData.size() + " 条数据");
                                    
                                    // 获取表的主键
                                    String primaryKeyColumn = tableInfo.getPrimaryKey();
                                    if (primaryKeyColumn == null) {
                                        System.err.println("表 " + tableName + " 没有主键，无法确定分片数据");
                                        return;
                                    }
                                    
                                    // 确定哪些数据应该存储在新服务器上
                                    List<String> shardingServers = new ArrayList<>(regionInfo.getRegionServers());
                                    // 从分片服务器列表中排除备份服务器
                                    shardingServers.remove(backupServer);
                                    
                                    int restoredCount = 0;
                                    for (Map<String, Object> row : allData) {
                                        Object primaryKeyValue = row.get(primaryKeyColumn);
                                        if (primaryKeyValue != null) {
                                            // 使用与客户端相同的分片逻辑确定数据位置
                                            String targetServer = shardingStrategy.getServerForKey(primaryKeyValue, shardingServers);
                                            
                                            if (newServer.equals(targetServer)) {
                                                newRegion.insert(tableName, row);
                                                restoredCount++;
                                            }
                                        }
                                    }
                                    
                                    System.out.println("成功将 " + restoredCount + " 条数据从备份服务器 " + 
                                                     backupServer + " 恢复到新服务器 " + newServer);
                                }
                            } catch (Exception e) {
                                System.err.println("从备份服务器恢复数据失败: " + e.getMessage());
                            }
                        } else {
                            System.err.println("备份服务器不可用，无法恢复数据");
                        }
                    } else {
                        System.err.println("没有可用服务器可以替代故障服务器");
                    }
                } else {
                    System.out.println("剩余服务器数量足够，无需添加新服务器");
                    //找出剩下的服务器
                    List<String> remainShardServers = regionInfo.getRegionServers();
                    //排除备份服务器获取分片服务器（不包含失联服务器）
                    remainShardServers.remove(backupServer);
                    //加入故障服务器得到所有分片服务器
                    List<String> shardServers =new ArrayList<>(remainShardServers);
                    shardServers.add(failedServer);

                    //从备份服务器恢复数据
                    try{
                        RegionService backupRegion = RPCUtils.getRegionService(backupServer);


                        List<Map<String, Object>> allData = backupRegion.select(tableName, null, new HashMap<>());
                        System.out.println("从备份服务器 " + backupServer + " 获取到 " + allData.size() + " 条数据");

                        // 获取表的主键
                        String primaryKeyColumn = tableInfo.getPrimaryKey();
                        if (primaryKeyColumn == null) {
                            System.err.println("表 " + tableName + " 没有主键，无法确定分片数据");
                            return;
                        }
                        System.out.println("开始进行数据恢复");
                        int restoredCount = 0;
                        for (Map<String, Object> row : allData) {
                            Object primaryKeyValue = row.get(primaryKeyColumn);
                            if (primaryKeyValue != null) {
                                // 使用与客户端相同的分片逻辑确定数据原本位置
                                String targetServer = shardingStrategy.getServerForKey(primaryKeyValue, shardServers);

                                //找到原本存放在故障服务器上的数据
                                if (failedServer.equals(targetServer)) {
                                    //根据分片策略在剩余的服务器上找到存放位置
                                    String newServer = shardingStrategy.getServerForKey(primaryKeyValue, remainShardServers);
                                    RegionService newRegion = RPCUtils.getRegionService(newServer);
                                    Message response =  newRegion.insert(tableName, row);
                                    restoredCount++;
                                }
                            }
                        }
                        System.out.println("成功将 " + restoredCount + " 条数据从备份服务器 " +
                                backupServer + " 恢复到其余分片服务器");
                    }catch (Exception e) {
                        System.err.println("从备份服务器恢复数据失败: " + e.getMessage());
                    }
                }
            }
            
            // 更新表区域信息到ZooKeeper
            masterService.updateTableRegionInfo(tableName, regionInfo);
            System.out.println("表 " + tableName + " 的恢复过程完成，更新后的服务器列表: " + regionInfo.getRegionServers());
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