/**
 * Created by 长春 on 2018/5/24.
 */


import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperClient3 implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;
    private static Stat stat = new Stat();
    private static int lastsessiontime;

    public static void main(String[] args) throws Exception {
        //String path = "/zk-book";
        zk = new ZooKeeper("172.20.3.1:2181", 5000, new ZooKeeperClient3());
        connectedSemaphore.await();
        zk.create("/SS", "java-cli".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String path1 = zk.create("/SS/PartitionId", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String path2 = zk.create("/SS/Lastsessiontime", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("Created [" + path1 + "] successfully");
        System.out.println("Created [" + path2 + "] successfully");
//        lastsessiontime = Integer.parseInt(new String(zk.getData(path, true, stat)));
//        zk.setData(path, "30000000".getBytes(), -1);
//        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
            if (EventType.None == event.getType() && null == event.getPath()) {
                connectedSemaphore.countDown();
                System.out.println("connected");
            } else if (event.getType() == EventType.NodeDataChanged) {
                System.out.println("enter change callback func: ");
                try {
                    lastsessiontime = Integer.parseInt(new String(zk.getData("/zk-book", true, stat)));
                    System.out.println("str: " + lastsessiontime);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

