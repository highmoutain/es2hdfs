

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperClient2 implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            ZooKeeper zk = new ZooKeeper("172.20.3.1:2181", 5000, new ZooKeeperClient2());
            System.out.println("Created zk");
            connectedSemaphore.await();

            String path = zk.create("/zk-node2", "java-cli".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("Created [" + path + "] successfully");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
        System.out.println("Event recieved: " + event);
        if (event.getState() == KeeperState.SyncConnected) {
            System.out.println("Connected");
            connectedSemaphore.countDown();
        }
    }
}
