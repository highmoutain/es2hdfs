

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperClient implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            ZooKeeper zk = new ZooKeeper("172.20.3.1:2181", 5000, new ZooKeeperClient());
            System.out.println("Created zk");
            connectedSemaphore.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
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
