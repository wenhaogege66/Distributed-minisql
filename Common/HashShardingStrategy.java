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
        
        // 获取可用于数据分片的服务器列表（排除备份服务器）
        List<String> shardingServers = getShardingServers(availableServers);
        if (shardingServers.isEmpty()) {
            // 如果没有足够的服务器进行分片，则使用所有可用服务器
            shardingServers = new ArrayList<>(availableServers);
        }
        
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
        
        // 使用与目前createTable方法相同的逻辑
        List<String> selectedServers = new ArrayList<>();
        
        // 识别备份服务器（使用排序后的最后一个）
        String backupServer = null;
        if (availableServers.size() >= 3) {
            List<String> sortedServers = new ArrayList<>(availableServers);
            Collections.sort(sortedServers);
            backupServer = sortedServers.get(sortedServers.size() - 1);
            
            // 确保备份服务器总是被包含在选中的服务器中
            selectedServers.add(backupServer);
        }
        
        // 从剩余服务器中选择负载最低的服务器作为数据分片服务器
        List<String> shardingServers = new ArrayList<>(availableServers);
        if (backupServer != null) {
            shardingServers.remove(backupServer);
        }
        
        // 按负载排序服务器
        shardingServers.sort((s1, s2) -> {
            int load1 = serverLoadMap.getOrDefault(s1, 0);
            int load2 = serverLoadMap.getOrDefault(s2, 0);
            return Integer.compare(load1, load2);
        });
        
        // 根据复制因子选择所需的分片服务器数量
        int shardingCount = Math.min(replicationFactor - (backupServer != null ? 1 : 0), shardingServers.size());
        
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
     * 获取备份服务器
     */
    public String getBackupServer(List<String> availableServers) {
        if (availableServers == null || availableServers.size() < 3) {
            return null; // 至少需要3个服务器才能有专用备份
        }
        
        // 复制可用服务器列表并排序
        List<String> servers = new ArrayList<>(availableServers);
        Collections.sort(servers);
        
        // 返回最后一个服务器作为备份服务器
        return servers.get(servers.size() - 1);
    }
    
    /**
     * 获取可用于数据分片的服务器列表（排除备份服务器）
     */
    public List<String> getShardingServers(List<String> availableServers) {
        if (availableServers == null || availableServers.size() <= reservedBackupServers) {
            return new ArrayList<>(availableServers); // 如果服务器不足，返回所有服务器
        }
        
        // 复制可用服务器列表并排序
        List<String> servers = new ArrayList<>(availableServers);
        Collections.sort(servers);
        
        // 移除用于备份的服务器
        if (servers.size() >= 3) {
            servers.remove(servers.size() - 1); // 移除最后一个作为备份服务器
        }
        
        return servers;
    }
    
    /**
     * 检查给定服务器是否为备份服务器
     */
    public boolean isBackupServer(String server, List<String> allServers) {
        String backupServer = getBackupServer(allServers);
        return backupServer != null && backupServer.equals(server);
    }
} 