package raftkeeper;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.options.DeleteOption;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by JackyWoo on 2021/3/2.
 */
public class EtcdTask implements Runnable {

    private final int id;
    private KV etcd;

    private final CountDownLatch latch;

    public EtcdTask(CountDownLatch latch, int id){
        this.latch = latch;
        this.id = id;
    }

    public static KV createClient() {
        // init zookeeper client
        return Client.builder().endpoints("http://" + Benchmark.nodes).build().getKVClient();
    }

    public void run() {
        try {

            etcd = createClient();
            // create root dir
            ByteSequence key = ByteSequence.from(Benchmark.ROOT_PATH.getBytes());
            ByteSequence value = ByteSequence.from(Benchmark.BLANK_BYTE);
            etcd.put(key, value).get();

            // pre run
            beforeRun(etcd, id);

            long t2 = System.nanoTime();
            int errorBatchCount = 0;

            long created = 0;

            while (System.nanoTime() < t2 + Benchmark.runDuration * 1000_000_000L){
                // send request by batch size 10 * BATCH_SIZE
                try {
                    int count = 0;
                    if(Benchmark.onlyCreate){
                        count = sendCreatedRequests(etcd, id, created);
                    } else {
                        count = sendRequests(etcd, id);
                    }
                    created += count;
                    Benchmark.totalCnt.addAndGet(count);
                }catch(Exception e){
                    if(errorBatchCount == 0){
                        e.printStackTrace();
                    }
                    errorBatchCount ++;
                }
            }

            if(errorBatchCount > 0){
                System.out.println("task " + id + " error bath count is " + errorBatchCount);
            }

            long t3 = System.nanoTime();
            Benchmark.totalTime.addAndGet((t3 - t2)/1000);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);

        } finally {
            try {
                if(etcd != null) {
                    afterRun(etcd, id);
                    etcd.close();
                }
            } catch (InterruptedException ignored) {
            }
        }

        latch.countDown();
    }

    /**
     * create:10%
     * set:40%
     * get:40%
     * delete:10%
     */
    private int sendRequests(KV etcd, int workerId)  {
        for(int i = 0; i< Benchmark.BATCH_SIZE; i++){
            String path = Benchmark.ROOT_PATH + "/" + workerId + "/" + Benchmark.NODE_PREFIX + i;
            create(etcd, path);
            for(int j=0; j<4; j++){
                setData(etcd, path);
                getData(etcd, path);
            }
            delete(etcd, path);
        }

        return 10 * Benchmark.BATCH_SIZE;
    }

    /**
     * only create operation
     */
    private int sendCreatedRequests(KV etcd, int workerId, long start)  {
        for(long i = start; i< start + Benchmark.BATCH_SIZE * 10; i++){
            String path = Benchmark.ROOT_PATH + "/" + workerId + "/" + Benchmark.NODE_PREFIX + i;
            create(etcd, path);
        }
        return Benchmark.BATCH_SIZE * 10;
    }

    private void deleteCreatedNodes(KV etcd, int workerId) throws InterruptedException, ExecutionException {
        String path = Benchmark.ROOT_PATH + "/" + workerId + "/";
        ByteSequence key = ByteSequence.from(path.getBytes());
        DeleteOption option = DeleteOption.newBuilder().withPrefix(key).build();
        etcd.delete(key, option).get();
    }

    private void create(KV etcd, String path) {
        long startTime = System.nanoTime();
        try {
            etcd.put(ByteSequence.from(path.getBytes()), ByteSequence.from(Benchmark.payload)).get();
        }catch(Exception e){
            Benchmark.errorCount.incrementAndGet();
        } finally {
            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt <0 || rt >= Benchmark.RT_CONTAINER_SIZE ) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void setData(KV etcd, String path) {
        long startTime = System.nanoTime();
        try {
            etcd.put(ByteSequence.from(path.getBytes()), ByteSequence.from(Benchmark.payload)).get();
        }catch(Exception e){
            Benchmark.errorCount.incrementAndGet();
        }finally {
            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt <0 || rt >= Benchmark.RT_CONTAINER_SIZE ) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void getData(KV etcd, String path) {
        long startTime = System.nanoTime();

        try {
            etcd.get(ByteSequence.from(path.getBytes())).get();
        }catch(Exception e){
            Benchmark.errorCount.incrementAndGet();
        }finally {
            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt <0 || rt >= Benchmark.RT_CONTAINER_SIZE ) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void delete(KV etcd, String path) {
        long startTime = System.nanoTime();
        try {
            etcd.delete(ByteSequence.from(path.getBytes())).get();
        }catch(Exception e){
            Benchmark.errorCount.incrementAndGet();
        }finally {
            long endTime = System.nanoTime();
            int rt = (int) ((endTime - startTime) / 100_000);
            if (rt <0 || rt >= Benchmark.RT_CONTAINER_SIZE ) {
                rt = Benchmark.RT_CONTAINER_SIZE - 1;
            }
            Benchmark.rtCount[rt].incrementAndGet();
        }
    }

    private void beforeRun(KV etcd, int workerId) throws InterruptedException {

        // create worker znode

        try {
            String path = Benchmark.ROOT_PATH + "/" + workerId;
            etcd.put(ByteSequence.from(path.getBytes()), ByteSequence.from(Benchmark.BLANK_BYTE)).get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    private void afterRun(KV etcd, int workerId) throws InterruptedException {
        // delete created nodes
        if(Benchmark.onlyCreate){
            try {
                deleteCreatedNodes(etcd, workerId);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        // delete worker znode
        try {
            String path = Benchmark.ROOT_PATH + "/" + workerId;
            etcd.delete(ByteSequence.from(path.getBytes())).get();
        } catch (ExecutionException e) {
        }
    }

    private static final int baseNodeCreatingThreadSize = 40;
    private static final long batchSize = Benchmark.initNodeSize /baseNodeCreatingThreadSize;
    private static ExecutorService executor;
    private static AtomicLong failedBaseNodes = new AtomicLong(0);

    public static void createBaseNodes() throws InterruptedException, ExecutionException {

        executor = Executors.newFixedThreadPool(baseNodeCreatingThreadSize);

        // create base znodes

        String basePath = Benchmark.BASE_NODE_ROOT + "/base_" + Benchmark.initNodePath + "_" + Benchmark.initNodeSize;

        KV etcd = createClient();
        long starTime = System.currentTimeMillis();

        try {
            etcd.put(ByteSequence.from(Benchmark.BASE_NODE_ROOT.getBytes()), ByteSequence.from(Benchmark.BLANK_BYTE)).get();
        } catch (InterruptedException | ExecutionException ignored) {

        }

        try {
            etcd.put(ByteSequence.from(basePath.getBytes()), ByteSequence.from(Benchmark.BLANK_BYTE)).get();
        } catch (InterruptedException | ExecutionException ignored) {

        }

        for(int i = 0; i< Benchmark.initNodeSize; i+=batchSize){
            long startIndex = i;
            etcd.put(ByteSequence.from((basePath + "/" + startIndex).getBytes()), ByteSequence.from(Benchmark.BLANK_BYTE)).get();
            executor.execute(() -> {
                KV etcdC = null;
                etcdC = createClient();
                for(long j = 0; j< batchSize; j++) {
                    try {
                        etcdC.put(ByteSequence.from((basePath + "/" + startIndex + "/" + j).getBytes()), ByteSequence.from(Benchmark.payload)).get();
                    } catch (Exception e) {
                        failedBaseNodes.incrementAndGet();
                    }
                }
                if(etcd != null){
                    etcdC.close();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);

        long endTime = System.currentTimeMillis();
        System.out.println(new Date());
        System.out.println("create base nodes " + (Benchmark.initNodeSize - failedBaseNodes.get()));
        System.out.println("create " + Benchmark.initNodeSize + " nodes, time cost " + (endTime-starTime)/1000 + " s");

    }


}
