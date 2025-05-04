package Common;

import java.util.*;

/**
 * 基于哈希的数据分片策略实现
 */
public class HashShardingStrategy implements ShardingStrategy {
    
    private static final long serialVersionUID = 1L;
    
    // 每个表的副本数，默认为2
    private int replicationFactor = 2;
    
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
        
        // 使用一致性哈希选择服务器
        int serverIndex = hash % availableServers.size();
        return availableServers.get(serverIndex);
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
        
        // 选择负载最低的服务器
        for (int i = 0; i < serversNeeded; i++) {
            selectedServers.add(sortedServers.get(i).getKey());
        }
        
        return selectedServers;
    }
} 