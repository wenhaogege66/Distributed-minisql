package Common;

import java.util.*;

/**
 * 基于哈希的数据分片策略实现
 */
public class HashShardingStrategy implements ShardingStrategy {
    
    private static final long serialVersionUID = 1L;
    
    // 每个表的副本数，默认为2
    private int replicationFactor = 2;
    // 保留用于备份的RegionServer数量
    private int reservedBackupServers = 1;
    
    public HashShardingStrategy() {
    }
    
    public HashShardingStrategy(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
    
    @Override
    public String getServerForKey(Object key, List<String> availableServers) {
        if (availableServers == null || availableServers.isEmpty()) {
            return null;
        }
        
        // 获取用于数据分片的服务器列表（排除备份服务器）
        List<String> shardingServers = getShardingServers(availableServers);
        if (shardingServers.isEmpty()) {
            // 如果没有足够的服务器进行分片，则使用所有可用服务器
            shardingServers = new ArrayList<>(availableServers);
        }

        Collections.sort(shardingServers);
        // 计算键的哈希值
        int hash = Math.abs(key.hashCode());
        
        // 使用哈希值选择服务器
        return shardingServers.get(hash % shardingServers.size());
    }
    
    @Override
    public List<String> getServersForReplication(String primaryServer, List<String> availableServers) {
        if (availableServers == null || availableServers.size() <= 1) {
            return new ArrayList<>();
        }
        
        // 复制可用服务器列表并排序
        List<String> servers = new ArrayList<>(availableServers);
        Collections.sort(servers);
        
        // 移除主服务器
        servers.remove(primaryServer);
        
        // 确保备份服务器总是在最后位置
        String backupServer = getBackupServer(availableServers);
        if (backupServer != null && !backupServer.equals(primaryServer)) {
            servers.remove(backupServer);
            servers.add(backupServer);
        }
        
        // 根据复制因子选择服务器
        int count = Math.min(replicationFactor - 1, servers.size());
        
        // 始终包含备份服务器
        if (backupServer != null && !servers.subList(servers.size() - count, servers.size()).contains(backupServer)) {
            // 如果备份服务器不在选中列表中，替换最后一个服务器
            servers.set(servers.size() - 1, backupServer);
        }
        
        return servers.subList(0, count);
    }
    
    @Override
    public List<String> selectServersForNewTable(String tableName, List<String> availableServers, 
                                              Map<String, Integer> serverLoadMap) {
        if (availableServers == null || availableServers.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<String> selectedServers = new ArrayList<>();
        
        // 获取用于数据分片的服务器列表（排除备份服务器）
        List<String> shardingServers = getShardingServers(availableServers);
        
        // 识别备份服务器（按名称排序后的最后一个）
        String backupServer = getBackupServer(availableServers);
        
        // 如果存在备份服务器，总是将其添加到所选服务器中
        if (backupServer != null) {
            selectedServers.add(backupServer);
            
            // 从分片服务器中移除备份服务器（确保不重复）
            shardingServers.remove(backupServer);
        }
        
        // 如果没有足够的服务器进行分片，使用所有可用服务器
        if (shardingServers.isEmpty()) {
            if (!selectedServers.isEmpty()) {
                return selectedServers; // 只有备份服务器
            }
            shardingServers = new ArrayList<>(availableServers);
        }
        
        // 按负载排序服务器
        shardingServers.sort((s1, s2) -> {
            int load1 = serverLoadMap.getOrDefault(s1, 0);
            int load2 = serverLoadMap.getOrDefault(s2, 0);
            return Integer.compare(load1, load2);
        });
        
        // 选择负载最低的服务器作为数据分片服务器
        // 当备份服务器已在selectedServers列表中时，只需要额外选择(replicationFactor-1)个服务器
        // 否则需要选择replicationFactor个服务器
        int neededShardingServers = backupServer != null ? 
                                   replicationFactor - 1 : 
                                   replicationFactor;
        
        // 确保不选择超过可用服务器数量
        int shardingCount = Math.min(neededShardingServers, shardingServers.size());
        
        // 添加分片服务器
        for (int i = 0; i < shardingCount; i++) {
            selectedServers.add(shardingServers.get(i));
        }
        
        return selectedServers;
    }
    
    @Override
    public int getReplicationFactor() {
        return replicationFactor;
    }
    
    /**
     * 获取备份服务器（按名称排序后的最后一个服务器）
     */
    public String getBackupServer(List<String> availableServers) {
        if (availableServers == null || availableServers.size() < 3) {
            return null;
        }
        
        List<String> sortedServers = new ArrayList<>(availableServers);
        Collections.sort(sortedServers);
        return sortedServers.get(sortedServers.size() - 1);
    }
    
    /**
     * 获取用于分片的服务器列表（即排除备份服务器后的服务器列表）
     */
    public List<String> getShardingServers(List<String> availableServers) {
        List<String> shardingServers = new ArrayList<>(availableServers);
        
        // 如果有足够的服务器，移除备份服务器
        if (availableServers.size() >= 3) {
            String backupServer = getBackupServer(availableServers);
            if (backupServer != null) {
                shardingServers.remove(backupServer);
            }
        }
        
        return shardingServers;
    }
    
    /**
     * 检查给定服务器是否为备份服务器
     */
    public boolean isBackupServer(String server, List<String> allServers) {
        String backupServer = getBackupServer(allServers);
        return backupServer != null && backupServer.equals(server);
    }
} 