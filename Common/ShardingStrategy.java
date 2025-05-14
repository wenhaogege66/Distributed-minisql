package Common;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 数据分片策略接口
 */
public interface ShardingStrategy extends Serializable {
    
    /**
     * 根据主键获取服务器
     * @param key 主键值
     * @param availableServers 可用的服务器列表
     * @return 选择的服务器
     */
    String getServerForKey(Object key, List<String> availableServers);
    
    /**
     * 获取数据复制的服务器列表
     * @param primaryServer 主服务器
     * @param availableServers 可用的服务器列表
     * @return 选择的服务器列表（不包括主服务器）
     */
    List<String> getServersForReplication(String primaryServer, List<String> availableServers);
    
    /**
     * 为新表选择服务器
     * @param tableName 表名
     * @param availableServers 可用的服务器列表 
     * @param serverLoadMap 服务器负载映射
     * @return 选择的服务器列表
     */
    List<String> selectServersForNewTable(String tableName, List<String> availableServers, 
                                         Map<String, Integer> serverLoadMap);
    
    /**
     * 获取复制因子
     * @return 复制因子
     */
    default int getReplicationFactor() {
        return 2; // 默认复制因子为2
    }
} 