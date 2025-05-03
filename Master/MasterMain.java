package Master;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Master启动类
 */
public class MasterMain {
    
    private static final int DEFAULT_PORT = 8000;
    
    public static void main(String[] args) {
        try {
            // 解析端口参数
            int port = DEFAULT_PORT;
            if (args.length > 0) {
                port = Integer.parseInt(args[0]);
            }
            
            // 设置RMI主机名，确保远程连接正常
            System.setProperty("java.rmi.server.hostname", "localhost");
            
            // 创建服务实例
            MasterServiceImpl masterService = new MasterServiceImpl();
            
            // 创建RMI注册表
            Registry registry = LocateRegistry.createRegistry(port);
            
            // 注册服务
            registry.rebind("MasterService", masterService);
            
            System.out.println("Master started on port " + port);
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.out.println("Shutting down Master...");
                    // 执行清理操作
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        } catch (Exception e) {
            System.err.println("Master exception: " + e.toString());
            e.printStackTrace();
        }
    }
} 