package com.jd.raft.benchmark;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.zookeeper.CreateMode.PERSISTENT;

/**
 * Created by wujianchao on 2021/3/2.
 */
public class ZooTask implements Runnable {

    private int id;//线程的编号
    private ZooKeeper zoo;//zookeeper连接

    private CountDownLatch latch;//线程

    public ZooTask(CountDownLatch latch, int id){
        this.latch = latch;
        this.id = id;
    }

    public static ZooKeeper createClient() throws IOException {
        // init zookeeper client
        return new ZooKeeper(Main.client, 1000000, null);
    }//初始化客户端

    public static void createRootPath() throws IOException {
        // init zookeeper client
        ZooKeeper zoo1 = createClient();
        try {
            zoo1.create(Main.ROOT_PATH, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException e) {
//                e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            zoo1.create(Main.CREATE_PATH, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException e) {
//                e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            zoo1.create(Main.CREATE_PATH+ "/"+"threadCount"+Main.threads, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException e) {
//                e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public static void createBaseRootPath() throws IOException {
        // init zookeeper client
        ZooKeeper zoo1 = createClient();
        try {
            zoo1.create(Main.BASE_NODE_ROOT , Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException e) {
//                e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() {//运行
        try {
            zoo = createClient();


            // create root dir
//            try {
//                zoo.create(Main.ROOT_PATH, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
//            } catch (KeeperException e) {
//                e.printStackTrace();
//            }
//            try {
//                zoo.create(Main.BASE_NODE_ROOT , Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
//            } catch (KeeperException e) {
//                e.printStackTrace();
//            }

            // pre run
            before(zoo, id);
            try {
                zoo.create(Main.CREATE_PATH+ "/"+"threadCount"+Main.threads+ "/" + id, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            } catch (KeeperException e) {

            }

            long t2 = System.nanoTime();
            int errorBatchCount = 0;
            int count = 0;
            if(Main.commandStr.equals("createBase")){
                try {
                    zoo.create(Main.BASE_NODE_ROOT + "/" + id, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
                } catch (KeeperException e) {
                    //System.out.println("create Base root "+id+" error!");
                }
                int batchNum = 0;
                for(int i=Main.baseStart;i<Main.baseSize;++i){
                    try {
                        zoo.create(Main.BASE_NODE_ROOT + "/" + id + "/" + i, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
                    } catch (KeeperException e) {

                    }
                    count = sendCreatedBaseRequests(zoo, id, i);
                    Main.totalCnt.addAndGet(count);
                }
            }
            else{
                int batchNum = 0;
                long endTime=System.nanoTime()+Main.duration*1000_000_000L;

                while (System.nanoTime()<endTime){//
                    // send request by batch size 10 * BATCH_SIZE
                    try {
                        if(Main.commandStr.equals("create")){//只创造
                            zoo.create(Main.CREATE_PATH+ "/"+"threadCount"+Main.threads+ "/" + id + "/" + batchNum, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
                            count = sendCreatedRequests(zoo, id, batchNum);
                        }
                        else if(Main.commandStr.equals("get")){
                            count = sendGetRequests(zoo, id ,batchNum);
                        }
                        else {
                            count = sendRequests(zoo, id);
                        }

                    }catch(Exception e){
                        //System.out.println("thread"+id+" catch "+"sendGetRequests");
                    }finally {
                        batchNum++;
                        Main.totalCnt.addAndGet(count);
                    }
                }
            }
            long t3 = System.nanoTime();

            if(errorBatchCount > 0){
                System.out.println("task " + id + " error bath count is " + errorBatchCount);
            }

            //Main.totalTime.addAndGet((t3 - t2)/1_000_000);//加上时间
        } catch (Exception e) {
            e.printStackTrace();
            //throw new RuntimeException(e);
        } finally {
            try {
                if(zoo != null) {
                    rmr(zoo,Main.ROOT_PATH +"/" + id);
                    rmr(zoo,Main.CREATE_PATH+ "/"+"threadCount"+Main.threads+ "/" + id);
                    zoo.close();
                }
            } catch (Exception e) {
            }
        }
        latch.countDown();
    }

    /**
     * create:10%
     * set:40%
     * get:40%
     * delete:10%
     * @return
     */
    private int sendRequests(ZooKeeper zoo, int workerId) {
        for(int i = 0; i< Main.BATCH_SIZE; i++){
            String path = Main.ROOT_PATH + "/" + workerId + "/" + Main.NODE_PREFIX + i;
            try {
                create(zoo, path);
                for(int j=0; j<4; j++){//执行四遍
                    setData(zoo, path);
                    getData(zoo, path);
                }
                delete(zoo, path);
            } catch (Exception e) {
                if(i == 0) {
                    e.printStackTrace();
                }
            }
        }
        return 10 * Main.BATCH_SIZE;
    }

    /**
     * create baseNode operation
     */
    private int sendCreatedBaseRequests(ZooKeeper zoo, int workerId, int batchIndex) throws Exception {
        //System.out.println("createBase");
        for(int  i = 0; i< Main.CREATE_BATCH_SIZE; i++){
            String path = Main.BASE_NODE_ROOT + "/" + workerId + "/" + batchIndex + "/" + Main.NODE_PREFIX + i;
            create(zoo, path);//创造
        }
        return Main.CREATE_BATCH_SIZE;
    }

    /**
     * only create operation
     */
    private int sendCreatedRequests(ZooKeeper zoo, int workerId, int batchIndex) throws Exception {
        for(int i = 0; i<Main.CREATE_BATCH_SIZE; i++){
            String path =  Main.CREATE_PATH+ "/"+"threadCount"+Main.threads + "/" + workerId + "/" + batchIndex+ "/"+Main.NODE_PREFIX + i;
            create(zoo, path);//创造
        }
        return Main.CREATE_BATCH_SIZE;
    }

    private int sendCreatedLZYRequests(ZooKeeper zoo, int workerId, int batchIndex) throws Exception {

        for(int i = 0; i<Main.CREATE_BATCH_SIZE; i++){
            String path = Main.ROOT_PATH + "/" + workerId + "/" + batchIndex+ "/"+Main.NODE_PREFIX + i;
            create(zoo, path);//创造
        }
        return Main.CREATE_BATCH_SIZE;
    }
    /**
     * get baseNode operation
     */
    private int sendGetRequests(ZooKeeper zoo, int workerId ,int batchIndex ) throws Exception {
        batchIndex%=Main.baseSize;
        for(int  i = 0; i< Main.CREATE_BATCH_SIZE; i++){
            String path = Main.BASE_NODE_ROOT + "/" + workerId + "/" + batchIndex + "/" + Main.NODE_PREFIX + i;
            getData(zoo, path);//创造
        }

        return Main.CREATE_BATCH_SIZE;
    }


    /**
    private int sendCreatedRequests(ZooKeeper zoo, int workerId, long start) throws Exception {
        for(long i = start; i< start + Main.BATCH_SIZE * 10; i++){
            String path = Main.ROOT_PATH + "/" + workerId + "/" + Main.NODE_PREFIX + i;
            create(zoo, path);//创造
        }
        return Main.BATCH_SIZE * 10;
    } */


    private void deleteCreatedNodes(ZooKeeper zoo, String path) throws Exception {
        List<String> nodes = null;
        try {
            nodes = zoo.getChildren(path, false);
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        if(nodes != null) {
            for (String node : nodes) {
                zoo.delete(path + "/" + node, -1);
            }
        }
    }

    public static void rmr(ZooKeeper zk,String path) throws Exception {
        //获取路径下的节点
        List<String> children = zk.getChildren(path, false);
        for (String pathCd : children) {
            //获取父节点下面的子节点路径
            String newPath = "";
            //递归调用,判断是否是根节点
            if (path.equals("/")) {
                newPath = "/" + pathCd;
            } else {
                newPath = path + "/" + pathCd;
            }
            rmr(zk,newPath);
        }
        //删除节点,并过滤zookeeper节点和 /节点
        if (path != null && !path.trim().startsWith("/zookeeper") && !path.trim().equals("/")) {
            zk.delete(path, -1);
            //打印删除的节点路径
            //System.out.println("被删除的节点为：" + path);
        }
    }





    private void create(ZooKeeper zoo, String path)  {
        long startTime = System.nanoTime();
        try {
            zoo.create(path, Main.payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        }catch(Exception e){
           //Main.errorCount.incrementAndGet();
            Main.errorCountVec[id][0].incrementAndGet();
        }finally {
            long endTime = System.nanoTime();
            Main.threadTpsVec[id][Math.min((int)((endTime-Main.runStartTime)/1000_000_000),Main.RT_TIME_SIZE-1)]++;
            int rt = (int) ((endTime - startTime)/50_000 );
            if (rt <0 || rt >= Main.RT_BUCKET_SIZE) {
                if((endTime - startTime)/1000_000_000>30){
                    Main.timeoutCountVec[id][0].incrementAndGet();
                }
                rt = Main.RT_BUCKET_SIZE - 1;
            }
            Main.rtBucketsVec[0][rt].incrementAndGet();//增加创造计数
        }
    }

    private void setData(ZooKeeper zoo, String path) {
        long startTime = System.nanoTime();
        try {
            zoo.setData(path, Main.payload, -1);
        }catch(Exception e){
            //Main.errorCount.incrementAndGet();
            Main.errorCountVec[id][1].incrementAndGet();
        } finally {
            long endTime = System.nanoTime();
            Main.threadTpsVec[id][Math.min((int)((endTime-Main.runStartTime)/1000_000_000),Main.RT_TIME_SIZE-1)]++;
            int rt = (int) ((endTime - startTime)/50_000);
            if (rt <0 || rt >= Main.RT_BUCKET_SIZE) {
                if((endTime - startTime)/1000_000_000>30){
                    Main.timeoutCountVec[id][1].incrementAndGet();
                }
                rt = Main.RT_BUCKET_SIZE - 1;
            }
            Main.rtBucketsVec[1][rt].incrementAndGet();//响应时间以间隔记录
        }
    }

    private void getData(ZooKeeper zoo, String path)  {
        long startTime = System.nanoTime();
        try {
            zoo.getData(path, null, null);
        }catch(Exception e){
            //Main.errorCount.incrementAndGet();
            Main.errorCountVec[id][2].incrementAndGet();
        }finally {
            long endTime = System.nanoTime();
            Main.threadTpsVec[id][Math.min((int)((endTime-Main.runStartTime)/1000_000_000),Main.RT_TIME_SIZE-1)]++;
            int rt = (int) ((endTime - startTime)/50_000);
            if (rt <0 || rt >= Main.RT_BUCKET_SIZE) {
                if((endTime - startTime)/1000_000_000>30){
                    Main.timeoutCountVec[id][2].incrementAndGet();
                }
                rt = Main.RT_BUCKET_SIZE - 1;
            }
            Main.rtBucketsVec[2][rt].incrementAndGet();
        }
    }

    private void delete(ZooKeeper zoo, String path){
        long startTime = System.nanoTime();
        try {
            zoo.delete(path, -1);
        }catch(Exception e){
            //Main.errorCount.incrementAndGet();
            Main.errorCountVec[id][3].incrementAndGet();
        }finally {
            long endTime = System.nanoTime();
            Main.threadTpsVec[id][Math.min((int)((endTime-Main.runStartTime)/1000_000_000),Main.RT_TIME_SIZE-1)]++;
            int rt = (int) ((endTime - startTime)/50_000);
            if (rt <0 || rt >= Main.RT_BUCKET_SIZE) {
                if((endTime - startTime)/1000_000_000>30){
                    Main.timeoutCountVec[id][3].incrementAndGet();
                }
                rt = Main.RT_BUCKET_SIZE - 1;
            }
            Main.rtBucketsVec[3][rt].incrementAndGet();
        }
    }

    private void before(ZooKeeper zoo, int workerId) throws InterruptedException {

        // create worker znode

        try {
            zoo.create(Main.ROOT_PATH + "/" + workerId, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException e) {

        }
    }

    public static void afterRun() throws Exception {
        // delete created nodes
        ZooKeeper zoo1 = createClient();
        String path = Main.ROOT_PATH;
        rmr(zoo1, path);
    }

    private static final int baseNodeCreatingThreadSize = 40;//线程创造个数
    private static final long batchSize = Main.baseSize/baseNodeCreatingThreadSize;
    private static ExecutorService executor;
    private static AtomicLong failedBaseNodes = new AtomicLong(0);

    public static void createBaseNodes() throws InterruptedException, KeeperException, IOException {
        executor = Executors.newFixedThreadPool(baseNodeCreatingThreadSize);
        //newFixedThreadPool创建一个定长线程池，可控制线程最大并发数，超出的线程会在队列中等待。

        // create base znodes

        String basePath = Main.BASE_NODE_ROOT + "/base_" + Main.baseID + "_" + Main.baseSize;

        long starTime = System.currentTimeMillis();

        ZooKeeper zoo = createClient();

        // create root dir
        try {
            zoo.create(Main.BASE_NODE_ROOT, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException e) {
//                        e.printStackTrace();
        }

        try {
            zoo.create(basePath , Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException | InterruptedException ignored) {

        }

        for(int i=0; i< Main.baseSize; i+=batchSize){
            long startIndex = i;
            try {
                zoo.create(basePath + "/" + startIndex, Main.BLANK_BYTE, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
            }catch(KeeperException.NodeExistsException ignored){

            }
            executor.execute(() -> {
                ZooKeeper zooC = null;
                try {
                    zooC = createClient();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                for(long j = 0; j< batchSize; j++) {
                    try {
                        zooC.create(basePath + "/" + startIndex + "/" + j, Main.payload, ZooDefs.Ids.OPEN_ACL_UNSAFE, PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {

                    }catch (Exception e){
                        failedBaseNodes.incrementAndGet();
                    }
                }
                if(zooC != null) {
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
        System.out.println("create base nodes " + (Main.baseSize - failedBaseNodes.get()));
        System.out.println("create " + Main.baseSize + " nodes, time cost " + (endTime-starTime)/1000 + " s");

    }

}
