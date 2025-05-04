package Common;

import Master.MasterService;
import RegionServer.RegionService;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RPC工具类，用于组件间通信
 */
public class RPCUtils {
    
    // 缓存已连接的服务
    private static final Map<String, Object> serviceCache = new ConcurrentHashMap<>();
    
    /**
     * 获取Master服务
     */
    public static MasterService getMasterService(String host, int port) throws RemoteException, NotBoundException {
        String key = "master:" + host + ":" + port;
        
        // 检查缓存
        if (serviceCache.containsKey(key)) {
            return (MasterService) serviceCache.get(key);
        }
        
        // 获取服务
        Registry registry = LocateRegistry.getRegistry(host, port);
        MasterService service = (MasterService) registry.lookup("MasterService");
        
        // 缓存服务
        serviceCache.put(key, service);
        
        return service;
    }
    
    /**
     * 获取RegionServer服务
     */
    public static RegionService getRegionService(String hostAndPort) throws RemoteException, NotBoundException {
        String key = "region:" + hostAndPort;
        
        // 检查缓存
        if (serviceCache.containsKey(key)) {
            return (RegionService) serviceCache.get(key);
        }
        
        // 解析主机名和端口
        String[] parts = hostAndPort.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        // 获取服务
        Registry registry = LocateRegistry.getRegistry(host, port);
        RegionService service = (RegionService) registry.lookup("RegionService");
        
        // 缓存服务
        serviceCache.put(key, service);
        
        return service;
    }
    
    /**
     * 获取RegionServer服务
     */
    public static RegionService getRegionService(String host, int port) throws RemoteException, NotBoundException {
        return getRegionService(host + ":" + port);
    }
    
    /**
     * 清除服务缓存
     */
    public static void clearCache() {
        serviceCache.clear();
    }
    
    /**
     * 从缓存中移除服务
     */
    public static void removeFromCache(String serviceKey) {
        serviceCache.remove(serviceKey);
    }
} 