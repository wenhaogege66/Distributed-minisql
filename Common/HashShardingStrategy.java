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
        
        // 计算键的哈希值
        int hash = Math.abs(key.hashCode());
        
        // 获取可用于数据分片的服务器列表（排除备份服务器）
        List<String> shardingServers = getShardingServers(availableServers);
        if (shardingServers.isEmpty()) {
            // 如果没有用于分片的服务器，则使用所有可用服务器
            shardingServers = availableServers;
        }
        
        // 使用一致性哈希选择服务器
        int serverIndex = hash % shardingServers.size();
        return shardingServers.get(serverIndex);
    }
    
    @Override
    public List<String> selectServersForNewTable(String tableName, List<String> availableServers, 
                                               Map<String, Integer> serverLoadMap) {
        if (availableServers == null || availableServers.isEmpty()) {
            return new ArrayList<>();
        }
        
        int serversNeeded = Math.min(replicationFactor, availableServers.size());
        List<String> selectedServers = new ArrayList<>(serversNeeded);
        
        // 按负载从低到高排序服务器
        List<Map.Entry<String, Integer>> sortedServers = new ArrayList<>();
        
        // 初始化所有服务器的负载数据
        for (String server : availableServers) {
            int load = serverLoadMap.getOrDefault(server, 0);
            sortedServers.add(new AbstractMap.SimpleEntry<>(server, load));
        }
        
        // 排序服务器 (按负载从低到高)
        sortedServers.sort(Comparator.comparingInt(Map.Entry::getValue));
        
        // 获取可用于数据分片的服务器列表（排除备份服务器）
        List<String> shardingServers = new ArrayList<>();
        int backupServerCount = 0;
        
        // 如果服务器数量大于等于3，需要保留至少一个服务器用于备份
        if (availableServers.size() >= 3) {
            // 先选择备份服务器（通常是负载较低的服务器）
            for (Map.Entry<String, Integer> entry : sortedServers) {
                if (backupServerCount < reservedBackupServers) {
                    // 将这个服务器标记为备份服务器
                    backupServerCount++;
                    // 同时也作为数据副本服务器
                    selectedServers.add(entry.getKey());
                } else {
                    // 其余服务器用于分片
                    shardingServers.add(entry.getKey());
                }
                
                // 如果已选择足够的服务器，退出循环
                if (selectedServers.size() >= serversNeeded) {
                    break;
                }
            }
            
            // 如果还需要更多服务器，从分片服务器中选择
            while (selectedServers.size() < serversNeeded && !shardingServers.isEmpty()) {
                selectedServers.add(shardingServers.remove(0));
            }
        } else {
            // 若服务器少于3个，则选择负载最低的服务器
            for (int i = 0; i < serversNeeded; i++) {
                selectedServers.add(sortedServers.get(i).getKey());
            }
        }
        
        return selectedServers;
    }
    
    /**
     * 获取可用于数据分片的服务器列表（排除备份服务器）
     */
    private List<String> getShardingServers(List<String> availableServers) {
        // 如果服务器数量小于3，所有服务器都可用于分片
        if (availableServers.size() < 3) {
            return new ArrayList<>(availableServers);
        }
        
        // 否则，排除用于备份的服务器
        List<String> shardingServers = new ArrayList<>(availableServers);
        // 按照服务器名称排序，以确保一致性（可以替换为更复杂的策略）
        Collections.sort(shardingServers);
        // 移除用于备份的服务器
        shardingServers.remove(shardingServers.size() - 1);
        
        return shardingServers;
    }
} 