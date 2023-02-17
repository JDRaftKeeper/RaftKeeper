package raftkeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * RaftKeeper follows the two constraints:
 * <p>
 *      1. each session must see its requests responded to in order
 *      2. all committed transactions must be handled in zxid order, across all sessions
 * <p>
 * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1505
 * <p>
 * This class test the first one.
 * <p>
 * Created by JackyWoo on 2021/3/2.
 */
public class SessionReadWriteConsistencyTest {

    private static final String ROOT_PATH = "/session_read_write_consistency_test";
    private static final int REQUEST_COUNT = 10000;
    private static final byte[] PAYLOAD = "".getBytes();

    public static void main(String[] args) throws Exception {
        String nodes = args[0];
        int thread_size = Integer.parseInt(args[1]);

        System.out.println("nodes " + nodes + ", thread_size " + thread_size);
        ZooKeeper zoo = new ZooKeeper(args[0], 30000, null);

        // 1. create ROOT_PATH
        if (zoo.exists(ROOT_PATH, false) == null) {
            zoo.create(ROOT_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 2. clean /session_read_write_consistency_test

        long childNum = 0;
        try {
            Stat stat = zoo.exists(ROOT_PATH, false);
            childNum = stat != null ? stat.getNumChildren() : 0;
        } catch (Exception ignored) {
        }

        int tryCount = 1;
        while (childNum > 0) {
            for (long i = childNum * (tryCount - 1); i < childNum * tryCount; i++) {
                try {
                    zoo.delete(ROOT_PATH + "/" + i, -1);
                } catch (KeeperException ignored) {
                }
            }
            tryCount++;
            try {
                Stat stat = zoo.exists(ROOT_PATH, false);
                childNum = stat != null ? stat.getNumChildren() : 0;
            } catch (Exception ignored) {
            }
        }

        // 3 run test

        final CountDownLatch latch = new CountDownLatch(thread_size);
        AtomicBoolean success = new AtomicBoolean(true);

        for (int i = 0; i < thread_size; i++) {
            int id = i;
            Thread t = new Thread(() -> {
                System.out.println("start thread " + id);

                for (long j = 0; j < REQUEST_COUNT / 10; j++) {
                    String path = ROOT_PATH + "/" + j;
                    try {
                        zoo.create(path, PAYLOAD, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
                        for (int m = 0; m < 4; m++) {
                            zoo.setData(path, PAYLOAD, -1);
                            zoo.getData(path, null, null);
                        }
                        zoo.delete(path, -1);
                    } catch (KeeperException.ConnectionLossException e) {
                        // Xid out of order
                        if (e.getMessage().contains("Xid out of order")) {
                            e.printStackTrace();
                            success.set(false);
                            break;
                        }
                    } catch (Exception ignored) {
                        // ignore other exception
                    }
                }
                latch.countDown();
            });

            t.start();
        }

        latch.await();
        try {
            zoo.close();
        } catch (Exception ignored) {
        }

        // 4. result
        if (success.get())
            System.out.println("Test success!");
        else
            System.out.println("Test failed!");
    }
}
