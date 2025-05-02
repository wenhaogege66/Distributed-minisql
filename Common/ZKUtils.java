package Common;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper工具类，用于与ZooKeeper集群交互
 */
public class ZKUtils {
    
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 30000;
    
    // ZNode路径常量
    public static final String MASTER_NODE = "/master";
    public static final String REGION_SERVERS_NODE = "/region-servers";
    public static final String TABLES_NODE = "/tables";
    
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedLatch = new CountDownLatch(1);
    // connectedLatch.await() 阻塞主线程，直到 ZooKeeper 与服务器连接成功，才会继续后续的操作,
    // connectedLatch.countDown() 当 ZooKeeper 与服务器连接成功时，调用该方法，通知主线程继续后续的操作
    /**
     * 创建ZooKeeper连接
     */
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedLatch.countDown();
                }
            }
        });
        connectedLatch.await();
    }
    
    /**
     * 关闭ZooKeeper连接
     */
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }
    
    /**
     * 创建ZNode节点
     */
    public void createNode(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        }
    }
    
    /**
     * 删除ZNode节点
     */
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat != null) {
            zooKeeper.delete(path, -1);
        }
    }
    
    /**
     * 获取节点数据
     */
    public byte[] getData(String path, Watcher watcher) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        return zooKeeper.getData(path, watcher, stat);
    }
    
    /**
     * 设置节点数据
     */
    public void setData(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data, -1);
    }
    
    /**
     * 获取子节点列表
     */
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(path, watcher);
    }
    
    /**
     * 检查节点是否存在
     */
    public boolean exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, watcher);
        return stat != null;
    }
    
    /**
     * 初始化ZooKeeper节点结构
     */
    public void initZKNodes() throws KeeperException, InterruptedException {
        // 创建根节点
        createNode(MASTER_NODE, new byte[0], CreateMode.PERSISTENT);
        createNode(REGION_SERVERS_NODE, new byte[0], CreateMode.PERSISTENT);
        createNode(TABLES_NODE, new byte[0], CreateMode.PERSISTENT);
    }
} 