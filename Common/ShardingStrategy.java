package Common;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 数据分片策略接口
 */
public interface ShardingStrategy extends Serializable {
    
    /**
     * 根据主键值确定数据应该存储在哪个RegionServer
     * @param key 主键值
     * @param availableServers 可用的RegionServer列表
     * @return 选择的RegionServer
     */
    String getServerForKey(Object key, List<String> availableServers);
    
    /**
     * 创建新表时选择RegionServer
     * @param tableName 表名
     * @param availableServers 可用的RegionServer列表
     * @param serverLoadMap 服务器负载情况 (server -> 表数量)
     * @return 选择的RegionServer列表
     */
    List<String> selectServersForNewTable(String tableName, List<String> availableServers, 
                                         Map<String, Integer> serverLoadMap);
} 