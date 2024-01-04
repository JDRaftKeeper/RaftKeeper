package raftkeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * Created by JackyWoo on 2021/3/2.
 */
public class ZooTask implements Runnable {

    private final int id;
    private ZooKeeper zoo;

    private final CountDownLatch latch;

    public ZooTask(CountDownLatch latch, int id) {
        this.latch = latch;
        this.id = id;
    }

    public static ZooKeeper createClient() throws IOException, InterruptedException {
        // init zookeeper client
        ZooKeeper zk = new ZooKeeper(Benchmark.nodes, 10000, event -> {});

        if (zk.getState() != ZooKeeper.States.CONNECTED) {
            System.out.println("Failed to connect server!");
            System.exit(1);
        }
        return zk;
    }

    public void run() {
        try {

            zoo = createClient();

            // create root dir
            try {
                zoo.create(Benchmark.ROOT_PATH, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
            } catch (KeeperException e) {
                e.printStackTrace();
            }

            // pre run
            beforeRun(zoo, id);

            long t2 = System.nanoTime();
            int errorBatchCount = 0;

            long created = 0;

            while (System.nanoTime() < t2 + Benchmark.runDuration * 1000_000_000L) {
                // send request by batch size 10 * BATCH_SIZE
                try {
                    int count;
                    if (Benchmark.onlyCreate) {
                        count = sendCreatedRequests(zoo, id, created);
                    } else {
                        count = sendMixedRequests(zoo, id);
                    }
                    created += count;
                    Benchmark.totalCnt.addAndGet(count);
                } catch (Exception e) {
                    if (errorBatchCount == 0) {
                        e.printStackTrace();
                    }
                    errorBatchCount++;
                }
            }

            if (errorBatchCount > 0) {
                System.out.println("task " + id + " error bath count is " + errorBatchCount);
            }

            long t3 = System.nanoTime();
            Benchmark.totalTime.addAndGet((t3 - t2) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);

        } finally {
            try {
                if (zoo != null) {
                    afterRun(zoo, id);
                    zoo.close();
                }
            } catch (Exception ignored) {
            }
        }

        latch.countDown();
    }

    private String generateKey(int workerId, long idx) {
        return Benchmark.ROOT_PATH + "/" + workerId + "/" + Benchmark.NODE_PREFIX + idx;
    }

    /**
     * create:10%
     * set:40%
     * get:40%
     * delete:10%
     *
     * @return sent request count
     */
    private int sendMixedRequests(ZooKeeper zoo, int workerId) {
        for (long i = 0; i < Benchmark.BATCH_SIZE; i++) {
            String path = generateKey(workerId, i);
            try {
                create(zoo, path);
                for (int j = 0; j < 4; j++) {
                    setData(zoo, path);
                    getData(zoo, path);
                }
                delete(zoo, path);
            } catch (Exception e) {
                if (i == 0) {
                    e.printStackTrace();
                }
            }
        }
        return 10 * Benchmark.BATCH_SIZE;
    }

    /**
     * only create operation
     */
    private int sendCreatedRequests(ZooKeeper zoo, int workerId, long start) throws Exception {
        for (long i = start; i < start + Benchmark.BATCH_SIZE * 10; i++) {
            String path = generateKey(workerId, i);
            create(zoo, path);
        }
        return Benchmark.BATCH_SIZE * 10;
    }

    private void deleteCreatedNodes(ZooKeeper zoo, int workerId) throws Exception {
        String workerPath = Benchmark.ROOT_PATH + "/" + workerId;

        long childNum = 0;
        try {
            Stat stat = zoo.exists(workerPath, false);
            childNum = stat != null ? stat.getNumChildren() : 0;
        } catch (Exception ignored) {
        }

        int tryCount = 1;
        while (childNum > 0) {
            for (long i = childNum * (tryCount - 1); i < childNum * tryCount; i++) {
                try {
                    zoo.delete(generateKey(workerId, i), -1);
                } catch (KeeperException ignored) {
                }
            }
            tryCount++;
            try {
                Stat stat = zoo.exists(workerPath, false);
                childNum = stat != null ? stat.getNumChildren() : 0;
            } catch (Exception ignored) {
            }
        }
    }

    private void create(ZooKeeper zoo, String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.create(path, Benchmark.payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {
            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt < 0 || rt >= Benchmark.RT_CONTAINER_SIZE) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void setData(ZooKeeper zoo, String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.setData(path, Benchmark.payload, -1);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {
            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt < 0 || rt >= Benchmark.RT_CONTAINER_SIZE) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void getData(ZooKeeper zoo, String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.getData(path, null, null);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {
            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt < 0 || rt >= Benchmark.RT_CONTAINER_SIZE) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void delete(ZooKeeper zoo, String path) throws Exception {
        long startTime = System.nanoTime();
        try {
            zoo.delete(path, -1);
        } catch (Exception e) {
            Benchmark.errorCount.incrementAndGet();
            throw e;
        } finally {

            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt < 0 || rt >= Benchmark.RT_CONTAINER_SIZE) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void beforeRun(ZooKeeper zoo, int workerId) throws Exception {
        try {
            zoo.create(Benchmark.ROOT_PATH + "/" + workerId, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException.NodeExistsException nodeExistsException) {
            // delete child nodes if exist
            deleteCreatedNodes(zoo, workerId);
            // recreate it to reset cversion
            try {
                zoo.delete(Benchmark.ROOT_PATH + "/" + workerId, -1);
            } catch (KeeperException ignored) {
            }
            try {
                zoo.create(Benchmark.ROOT_PATH + "/" + workerId, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            } catch (KeeperException ignored) {
            }
        }
    }

    private void afterRun(ZooKeeper zoo, int workerId) throws Exception {
        // delete created nodes
        deleteCreatedNodes(zoo, workerId);

        // delete worker znode
        try {
            zoo.delete(Benchmark.ROOT_PATH + "/" + workerId, -1);
        } catch (KeeperException ignored) {
        }
    }

    private static final int baseNodeCreatingThreadSize = 40;
    private static final long batchSize = Benchmark.initNodeSize / baseNodeCreatingThreadSize;
    private static final AtomicLong failedBaseNodes = new AtomicLong(0);

    public static void createBaseNodes() throws InterruptedException, KeeperException, IOException {
        ExecutorService executor = Executors.newFixedThreadPool(baseNodeCreatingThreadSize);
        // create base znodes
        String basePath = Benchmark.BASE_NODE_ROOT + "/" + Benchmark.initNodePath;

        long starTime = System.currentTimeMillis();
        ZooKeeper zoo = createClient();

        // create root dir
        try {
            zoo.create(Benchmark.BASE_NODE_ROOT, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException ignored) {
        }

        try {
            zoo.create(basePath, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException | InterruptedException ignored) {

        }

        for (int i = 0; i < Benchmark.initNodeSize; i += batchSize) {
            long startIndex = i;
            try {
                zoo.create(basePath + "/" + startIndex, Benchmark.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {

            }
            executor.execute(() -> {
                ZooKeeper zooC = null;
                try {
                    zooC = createClient();
                } catch (IOException | InterruptedException ignored) {
                }
                for (long j = 0; j < batchSize; j++) {
                    try {
                        assert zooC != null;
                        zooC.create(basePath + "/" + startIndex + "/" + j, Benchmark.payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
                    } catch (KeeperException.NodeExistsException ignored) {

                    } catch (Exception e) {
                        failedBaseNodes.incrementAndGet();
                    }
                }
                if (zooC != null) {
                    try {
                        zooC.close();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);

        long endTime = System.currentTimeMillis();
        System.out.println(new Date());
        System.out.println("create base nodes " + (Benchmark.initNodeSize - failedBaseNodes.get()));
        System.out.println("create " + Benchmark.initNodeSize + " nodes, time cost " + (endTime - starTime) / 1000 + " s");

    }

}
