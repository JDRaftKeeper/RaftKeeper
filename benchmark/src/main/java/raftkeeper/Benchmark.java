package raftkeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by JackyWoo on 2021/3/1.
 */
public class Benchmark {

    public static void main(String[] args) {
        Benchmark main = new Benchmark();
        try {
            // args: target nodes thread_size payload_size run_duration(second) only_create
            main.benchmark(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static final byte[] BLANK_BYTE = "".getBytes();
    public static final int BATCH_SIZE = 100;
    public static final String ROOT_PATH = "/benchmark";
    public static final String BASE_NODE_ROOT = "/benchmark_base_node_root";
    // to keep key size 256
    public static String NODE_PREFIX;

    static {
        byte[] bytes = new byte[2];
        for (int i = 0; i < 2; i++) {
            bytes[i] = '0';
        }
        NODE_PREFIX = new String(bytes);
    }


    // benchmark config
    public static String target;
    public static String nodes;
    public static int thread_size;
    public static int payloadSize;
    // second
    public static int runDuration;
    public static boolean onlyCreate = true;
    // how many nodes to create before benchmark
    public static long initNodeSize = 0;
    // base nodes path, all initialize nodes will be placed at /benchmark/${baseNodePath}
    public static String initNodePath = "init_nodes";


    public static byte[] payload;

    public static final AtomicLong totalCnt = new AtomicLong(0);
    // microsecond
    public static final AtomicLong totalTime = new AtomicLong(0);
    public static long wallTime = 0;

    // for tp50 tp90 tp99 tp999
    public static final int RT_CONTAINER_SIZE = 10000;
    // 100 * microsecond
    public static final AtomicLong[] rtCount = new AtomicLong[RT_CONTAINER_SIZE];
    public static final AtomicLong errorCount = new AtomicLong(0);

    // benchmark indexes
    private int tps;
    // microsecond
    private double avgRT = 0D;
    private double failRate = 0D;

    private double tp30 = 0D;
    private double tp50 = 0D;
    private double tp90 = 0D;
    private double tp99 = 0D;
    private double tp999 = 0D;


    /**
     * API
     */
    public void benchmark(String[] args) throws Exception {
        parseConfig(args);
        run();
        calculateIndexes();
        print();
        System.exit(0);
    }

    public void parseConfig(String[] args) {
        target = args[0];
        nodes = args[1];
        thread_size = Integer.parseInt(args[2]);
        payloadSize = Integer.parseInt(args[3]);
        runDuration = Integer.parseInt(args[4]);
        onlyCreate = Boolean.parseBoolean(args[5]);
        if (args.length > 6) {
            initNodeSize = Long.parseLong(args[6]);
            initNodePath = args[7];
        }

        System.out.println("\n================= Benchmark config =================");
        System.out.println(target + " : " + nodes);
        System.out.print("threads: " + thread_size);
        System.out.print(",\tpayload_size: " + payloadSize);
        System.out.print(",\trun_duration: " + runDuration + "s");
        System.out.print(",\tonly_create: " + onlyCreate);
        System.out.print(",\tinit_node_size: " + initNodeSize);
        System.out.print(",\tinit_node_path: " + initNodePath);
        System.out.println("\n");
        String modeStr = "mode : ";
        if (initNodeSize == 0) {
            modeStr += "benchmark";
            if (onlyCreate) {
                modeStr += " create-100%";
            } else {
                modeStr += " create-10% set-40% get-40% delete-10%";
            }
        } else {
            modeStr += "import data";
        }
        System.out.println(modeStr);
        System.out.println("\n");
    }

    private void calculateIndexes() {

        tps = (int) (totalCnt.get() / ((totalTime.get() / thread_size) / 1000_000));
        avgRT = totalTime.get() / totalCnt.get();
        failRate = 100 * ((1.0 * errorCount.get()) / totalCnt.get());


        long summ = 0;
        for (int i = 0; i < RT_CONTAINER_SIZE; i++) {
            summ += rtCount[i].get();
        }
        if (summ != totalCnt.get()) {
            System.out.println("sum of rtCount " + summ);
        }

        long sum = 0L;
        double tp = 0D;

        for (int i = 0; i < RT_CONTAINER_SIZE; i++) {
            sum += rtCount[i].get();
            tp = ((double) sum) / summ;
            if (tp30 == 0 && tp >= 0.30D) {
                tp30 = i * 100;
            }
            if (tp50 == 0 && tp >= 0.50D) {
                tp50 = i * 100;
            }
            if (tp90 == 0 && tp >= 0.90D) {
                tp90 = i * 100;
            }
            if (tp99 == 0 && tp >= 0.99D) {
                tp99 = i * 100;
            }
            if (tp >= 0.999D) {
                tp999 = i * 100;
                break;
            }
        }
    }

    private void print() {
        System.out.println("\n\n================= Result (Time measured in microsecond.) =================");
        System.out.println("thread_size,tps,avgRT(microsecond),TP90(microsecond),TP99(microsecond),TP999(microsecond),failRate");
        System.out.println(thread_size + "," + tps + "," + avgRT + "," + tp90 + "," + tp99 + "," + tp999 + "," + failRate);
        System.out.println("\n");

        System.out.println("total requests: " + totalCnt.get());
        System.out.println("fail count: " + errorCount.get());
        System.out.println("total time cost: " + totalTime.get() + " microsecond");
        System.out.println("wallTime: " + wallTime / 1000 / 1000 + " second");
    }

    /**
     * run benchmark
     */
    private void run() throws InterruptedException, IOException, KeeperException, ExecutionException {
        long startTime = System.nanoTime();
        // init rt container

        for (int i = 0; i < RT_CONTAINER_SIZE; i++) {
            rtCount[i] = new AtomicLong(0);
        }

        // generate payload
        Benchmark.generatePayload();

        // run benchmark
        CountDownLatch latch = new CountDownLatch(thread_size);

        if (target.equals("zookeeper") || target.equals("raftkeeper")) {
            if (initNodeSize != 0) {
                ZooTask.createBaseNodes();
                System.exit(0);
            } else {
                for (int i = 0; i < thread_size; i++) {
                    Thread thread = new Thread(new ZooTask(latch, i), "worker-" + i);
                    thread.start();
                }
            }
        } else if (target.equals("etcd")) {
            if (initNodeSize != 0) {
                EtcdTask.createBaseNodes();
                System.exit(0);
            } else {
                for (int i = 0; i < thread_size; i++) {
                    Thread thread = new Thread(new EtcdTask(latch, i), "worker-" + i);
                    thread.start();
                }
            }
        } else {
            throw new RuntimeException("Invalid target!");
        }

        latch.await();
        long endTime = System.nanoTime();
        wallTime = (endTime - startTime) / 1000;
    }

    public static void generatePayload() {
        payload = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            payload[i] = (byte) 0;
        }
    }
}
