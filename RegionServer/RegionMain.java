package RegionServer;

import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * RegionServer启动类
 */
public class RegionMain {
    
    private static final int DEFAULT_PORT = 9000;
    
    public static void main(String[] args) {
        try {
            // 解析端口参数
            int port = DEFAULT_PORT;
            if (args.length > 0) {
                port = Integer.parseInt(args[0]);
            }
            
            // 获取主机名
            String hostname = InetAddress.getLocalHost().getHostName();
            
            // 创建服务实例
            RegionServiceImpl regionService = new RegionServiceImpl(hostname, port);
            
            // 创建RMI注册表
            Registry registry = LocateRegistry.createRegistry(port);
            
            // 注册服务
            registry.rebind("RegionService", regionService);
            
            System.out.println("RegionServer started on " + hostname + ":" + port);
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.out.println("Shutting down RegionServer...");
                    // 执行清理操作
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        } catch (Exception e) {
            System.err.println("RegionServer exception: " + e.toString());
            e.printStackTrace();
        }
    }
} 