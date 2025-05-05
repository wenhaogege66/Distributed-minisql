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
            try {
                // 测试连接是否仍然有效
                RegionService service = (RegionService) serviceCache.get(key);
                service.heartbeat(); // 如果连接断开，会抛出异常
                return service;
            } catch (Exception e) {
                // 连接不可用，从缓存中移除
                System.err.println("缓存的RegionServer连接失效: " + hostAndPort);
                serviceCache.remove(key);
                // 继续尝试重新连接
            }
        }
        
        // 解析主机名和端口
        String[] parts = hostAndPort.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        int retryCount = 3;
        int retryDelayMs = 1000; // 初始延迟1秒
        
        while (retryCount > 0) {
            try {
                // 获取服务
                Registry registry = LocateRegistry.getRegistry(host, port);
                RegionService service = (RegionService) registry.lookup("RegionService");
                
                // 测试连接
                if (service.heartbeat()) {
                    // 缓存服务
                    serviceCache.put(key, service);
                    return service;
                } else {
                    throw new RemoteException("RegionServer连接测试失败");
                }
            } catch (Exception e) {
                retryCount--;
                System.err.println("连接RegionServer " + hostAndPort + " 失败，剩余重试次数: " + retryCount);
                
                if (retryCount == 0) {
                    throw new RemoteException("无法连接到RegionServer: " + hostAndPort, e);
                }
                try {
                    // 使用指数退避策略
                    Thread.sleep(retryDelayMs);
                    retryDelayMs *= 2; // 每次失败后将延迟时间加倍
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RemoteException("连接RegionServer被中断", ie);
                }
            }
        }
        throw new RemoteException("无法连接到RegionServer: " + hostAndPort);
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