package com.jd.raft.benchmark;

import org.apache.zookeeper.KeeperException;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Created by wujianchao on 2021/3/1.
 */
public class Main {

    public static void main(String[] args) {
        Main main = new Main();
        try {
//             System.out.println("=================");
            main.benchmark(args);//主程序入口
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//---------------------------------benchmark config 程序运行时候参入参数，控制程序---------------------------------------
    public static String target;
    public static String client;
    public static int threads;
    public static int payloadSize;//插入节点数据大小
    public static int duration;//时间单位秒
    public static String commandStr = "mixed";
    public static int baseSize = 0;    // how many nodes to create before benchmark
    public static int baseStart = 0;
    public static int baseID = 0;    // base nodes id, all base nodes will be placed at /benchmark/base_${baseID}
    //public static AtomicInteger batchCnt = new AtomicInteger(0);

//---------------------------------end benchmark config---------------------------------------



//---------------------------------benchmark default config 在程序内修改的参数的，会影响程序运行性能的参数---------------------------------------
    public static final int PATH_SIZE = 50;//路径字符串字节数
    public static final int BATCH_SIZE = 100;//每次写入批次
    public static final int CREATE_BATCH_SIZE = 1000;//每次创造写入的批次
    public static final String ROOT_PATH = "/benchmark";//定义测试的路径
    public static final String CREATE_PATH = "/create";//定义测试的路径
    public static final String BASE_NODE_ROOT = "/benchmark_base_node_root";//写入基础节点的路径
    public static final int RT_TIME_SIZE = 1000 ;//每个线程，最大统计秒数
    public static final int RT_BUCKET_SIZE = 20_000;//计算时间桶的大小，太大会占用内存，单位取50微秒，最大不超过1ms
    //public static final int RT_BUCKET_SIZE = 100_000;//计算时间桶的大小，太大会占用内存，单位取微秒
//---------------------------------end benchmark default config---------------------------------------



//---------------------------------benchmark data---------------------------------------
    public static byte[] payload;//value
    public static String NODE_PREFIX;//路径字符串
    static {
        byte[] bytes = new byte[PATH_SIZE];
        for(int i=0;i<PATH_SIZE;i++){
            bytes[i] = 'a';
        }
        NODE_PREFIX = new String(bytes);
//         System.out.println("node prefix size " + NODE_PREFIX.getBytes().length);
    }
    public static final byte[] BLANK_BYTE = "".getBytes();//空数据，为了创造空节点
//---------------------------------end benchmark data---------------------------------------



//---------------------------------benchmark statistical data 程序运行记录的结果---------------------------------------
    public static final AtomicLong totalCnt = new AtomicLong(0);//统计总请求的个数
    public static final AtomicLong totalTime = new AtomicLong(0);//统计所有请求的运行时间

    public static int[][] threadTpsVec;//记录每个线程的每秒钟完成请求个数，用于后续计算tps

    public static long runStartTime;//程序参数处理完，初始化完成后，运行run（）的时间。
    public static long runFinishTime;//运行完run（）的时间。

    //public static AtomicLong[] createRTBucket;//记录相应操作响应时间的桶，桶间粒度为微秒
    //public static AtomicLong[] setRTBucket;//记录相应操作响应时间的桶，桶间粒度为微秒
    //public static AtomicLong[] getRTBucket;//记录相应操作响应时间的桶，桶间粒度为微秒
    //public static AtomicLong[] deleteRTBucket;//记录相应操作响应时间的桶，桶间粒度为微秒
    //public static final AtomicLong[] rtCount = new AtomicLong[RT_BUCKET_SIZE];//记录相应时间的数组，相应时间大小
    public static final AtomicInteger[][] rtBucketsVec = new AtomicInteger[4][RT_BUCKET_SIZE];//记录对应基本操作的响应时间

    //public static final AtomicLong errorCount = new AtomicLong(0);//记录错误
    public static AtomicLong[][] errorCountVec ;//记录每个操作的错误个数

    public static AtomicLong[][] timeoutCountVec ;//记录每个操作的超时个数

//---------------------------------end benchmark statistical data---------------------------------------


//---------------------------------benchmark result 通过calculateIndexes()计算的到的结果---------------------------------------
    private long totalRT = 0l;//总响应时间
    private long outBucketCount=0l;
    private long[] rtVec=new long[4];//每个操作的响应时间
    private long sumCount = 0;//总响应个数（去除超时和错误）
    private long sumCountVec[] = new long[4];//每个操作的响应个数（去除超时和错误）
    private long errorCount=0l;//总错误个数
    private long timeoutCount=0l;//总超时个数
    private int avgTps=0;//平均tps
    private int maxTps=0;//最大tps
    private double avgRT = 0D;//平均响应时间
    private double failRate = 0D;//计算失败率
    private double tp30 = 0D;
    private double tp50 = 0D;
    private double tp90 = 0D;
    private double tp99 = 0D;
    private double tp999 = 0D;
    private double[] otp30 = new double[4];//记录四个不同操作
    private double[] otp50 = new double[4];
    private double[] otp90 = new double[4];
    private double[] otp99 = new double[4];
    private double[] otp999 = new double[4];
    private double[] avgRTVec =new double[4];
    private int[]tpsVec;
//---------------------------------end benchmark result---------------------------------------

    /**
     * API
     */
    public void benchmark(String[] args) throws Exception {//测试主函数
        parseConfig(args);//解析主程序参数，并且对参数进行赋值
        Initialization();//初始化变量
        run();//运行测试程序
        calculateIndexes();//计算结果
        print();//打印结果
        System.exit(0);
    }

    public void parseConfig(String[] args)  {
        target = args[0];
        client = args[1];
        threads = Integer.parseInt(args[2]);
        payloadSize = Integer.parseInt(args[3]);
        duration = Integer.parseInt(args[4]);
        //batchCnt.set(Integer.parseInt(args[4])*threads);
        commandStr = args[5];
        if(commandStr.equals("clear")){
            try{
                ZooTask.afterRun();
            }catch (Exception e) {
                e.printStackTrace();
            }
            System.exit(0);
        }
        if(args.length > 6){
            baseSize = Integer.parseInt(args[6]);
            baseStart= Integer.parseInt(args[7]);
        }

//         System.out.println("\n================= Config =================");
//         System.out.println("target: " + target);
//         System.out.println("client: " + client);
//         System.out.println("threads: " + threads);
//         System.out.println("payloadSize: " + payloadSize);
//         System.out.println("duration: " + duration + " s");
//         //System.out.println("batchCnt: " + batchCnt );
//         System.out.println("model: " + commandStr);//只创造
//         System.out.println("baseSize: " + baseSize);
//         System.out.println("baseStart: " + baseStart);
//         System.out.println("\n");
        String modeStr = "mode : ";
        if(baseSize == 0){
            modeStr += "benchmark";
            if(commandStr.equals("create")){
                modeStr += " create-100%";
            }else{
                modeStr += " create-10% set-40% get-40% delete-10%";
            }
        }else{
            modeStr += "import data";
        }
//         System.out.println(modeStr);
//         System.out.println("\n");
    }

    private void Initialization(){
        // init rt container
        threadTpsVec = new int[threads][RT_TIME_SIZE];
        errorCountVec = new AtomicLong[threads][4];
        timeoutCountVec = new AtomicLong[threads][4];
        for(int i=0;i<4;++i){
            for(int j = 0; j< RT_BUCKET_SIZE; ++j){
                rtBucketsVec[i][j] = new AtomicInteger(0);
            }
        }

        for(int i=0;i<threads;++i){
            for(int j=0;j<4;++j){
                errorCountVec[i][j]=new AtomicLong(0);
                timeoutCountVec[i][j]=new AtomicLong(0);
            }
        }

        // generate payload
        payload = new byte[payloadSize];
        for(int i=0; i<payloadSize; i++){
            payload[i] = (byte)0;
        };
    }

    private void run() throws Exception {
//         System.out.println("start running!!!!!!!!!!!!!!");
        long startTime = System.nanoTime();
        // run benchmark

        CountDownLatch latch = new CountDownLatch(threads);
        try{
            ZooTask.createRootPath();
        } catch (Exception e) {
            throw e;
        }

        ZooTask.createBaseRootPath();
//         System.out.println("start running!!!!!!!!!!!!!!");
        runStartTime = System.nanoTime();
        Thread[] threadVec=new Thread[threads];
        if(target.equals("zookeeper") | target.equals("raft")){//如果等于zk和raft

            for (int i = 0; i < threads; i++) {
                threadVec[i] = new Thread(new ZooTask(latch, i), "worker-" + i);
                threadVec[i].start();
            }
        } else if(target.equals("etcd")){
            if(baseSize != 0) {
                //EtcdTask.createBaseNodes();
                System.exit(0);
            }else {
                for (int i = 0; i < threads; i++) {
                    Thread thread = new Thread(new EtcdTask(latch, i), "worker-" + i);
                    thread.start();
                }
            }
        } else {
            throw new RuntimeException("Invalid target!");
        }
        if(commandStr.equals("createBase")){
            latch.await();
        }
        else{
            latch.await(1_000_000,TimeUnit.MILLISECONDS);
        }
        runFinishTime = System.nanoTime();
//
    }

    private void calculateIndexes() {
//         System.out.println("start calculate!!!!!!!!!!!!!!");
        for(int i=0;i<4;++i){
            for(int j = 0; j< RT_BUCKET_SIZE; ++j){
                sumCountVec[i] += rtBucketsVec[i][j].get();
            }
        }

        //统计各个操作的总响应时间
        for(int i=0;i<4;++i){
            for(int j = 0; j< RT_BUCKET_SIZE; ++j){
                rtVec[i]+=j* rtBucketsVec[i][j].get();
            }
        }

        for(int i=0;i<4;++i){
            if(sumCountVec[i]==0){
                avgRTVec[i]=0;
            }
            else{
                avgRTVec[i]=rtVec[i]/sumCountVec[i];
            }
        }

        for(int i=0;i<threads;++i){
            for(int j=0;j<4;++j){
                errorCount+=errorCountVec[i][j].get();
                timeoutCount+= timeoutCountVec[i][j].get();
            }
        }

        for(int i=0;i<4;++i){
            totalRT+=rtVec[i];
            sumCount+=sumCountVec[i];
        }

        if(sumCount != totalCnt.get()){
//             System.out.println("sum of rtCount " + sumCount);
        }

        failRate = 100 * ((1.0 * errorCount) / totalCnt.get());//失败率
        avgTps = (int) (totalCnt.get()/(runFinishTime-runStartTime));
        avgRT = totalRT / totalCnt.get();



        long sum = 0L;//计算小于当前时刻响应个数
        double tp = 0D;//计算当前的tp
        for(int i = 0; i< RT_BUCKET_SIZE; i++){
            for(int j=0; j<4; ++j){
                sum += rtBucketsVec[j][i].get();
            }
            tp = ((double)sum) / sumCount;
            if(tp30 == 0 && tp >= 0.30D){
                tp30 = i;
            }
            if(tp50 == 0 && tp >= 0.50D){
                tp50 = i;
            }
            if(tp90 == 0 && tp >= 0.90D){
                tp90 = i;
            }
            if(tp99 == 0 && tp >= 0.99D){
                tp99 = i;
            }
            if(tp >= 0.999D){
                tp999 = i;
                break;
            }
        }

        //同上，不过计算的是每个操作
        long[] sumVec = new long[4];
        double[] tpVec = new double[4];
        for(int i=0;i<4;++i){
            if(sumCountVec[i]==0){
                continue;
            }
            for(int j = 0; j< RT_BUCKET_SIZE; j++){
                sumVec[i] += rtBucketsVec[i][j].get();
                tpVec[i] = ((double)sumVec[i]) / sumCountVec[i];
                if(otp30[i] == 0 && tpVec[i] >= 0.30D){
                    otp30[i] = j;
                }
                if(otp50[i] == 0 && tpVec[i] >= 0.50D){
                   otp50[i] = j;
                }
                if(otp90[i]== 0 && tpVec[i] >= 0.90D){
                    otp90[i] = j;
                }
                if(otp99[i] == 0 && tpVec[i] >= 0.99D){
                    otp99[i] = j;
                }
                if(tpVec[i] >= 0.999D){
                    otp999[i] = j;
                    break;
                }
            }
        }

        for(int i=0;i<4;++i){
            outBucketCount+=rtBucketsVec[i][RT_BUCKET_SIZE-1].get();
        }


        tpsVec=new int[RT_TIME_SIZE];
        for(int i=0;i<RT_TIME_SIZE-1;++i){
            for(int j=0;j<threads;++j){
                tpsVec[i]+= threadTpsVec[j][i];
            }
            maxTps=Math.max(maxTps,tpsVec[i]);
        }
        int[] tempTpsVec = IntStream.of(tpsVec)          // 变为 IntStream
                .boxed()           // 变为 Stream<Integer>
                .sorted(Comparator.reverseOrder()) // 按自然序相反排序
                .mapToInt(Integer::intValue)       // 变为 IntStream
                .toArray();
        int temp_num = RT_TIME_SIZE/10;
        int temp_sum=0;
        for (int i=0;i<temp_num;++i){
            temp_sum+=tempTpsVec[i];
        }
        maxTps = temp_sum/temp_num;
        for(int j=0;j<threads;++j){
            timeoutCount+=threadTpsVec[j][RT_TIME_SIZE-1];
        }
    }

    private void print() throws IOException {//打印结果
//         System.out.println("\n\n================= Result =================");
//         System.out.println("measured in microsecond");
//         System.out.println("thread_size,avgTps,avgRT(microsecond),TP90(microsecond),TP99(microsecond),TP999(microsecond),timeoutCount,failRate");
//         System.out.println(threads+","+avgTps+","+avgRT+","+tp90+","+tp99+","+tp999+","+timeoutCount+","+failRate);
//         System.out.println("\n");
//         System.out.println("total  requests: " + totalCnt.get());
//         System.out.println("fail count: " + errorCount);
//         System.out.println("fail count: " + errorCount);

        //写每个线程的tps文件
//         String threadTpsFilePath = "threadTPSResult"+"-"+threads+"-"+commandStr+"-"+baseSize+".txt";
//         File fileTT = new File(threadTpsFilePath);
//         FileOutputStream fosTT = null;
//         if(!fileTT.exists()){
//             fileTT.createNewFile();//如果文件不存在，就创建该文件
//             fosTT = new FileOutputStream(fileTT);//首次写入获取
//         }else{
//             //如果文件已存在，那么就在文件末尾追加写入
//             fosTT = new FileOutputStream(fileTT,true);//这里构造方法多了一个参数true,表示在文件末尾追加写入
//         }
//         OutputStreamWriter oswTT = new OutputStreamWriter(fosTT, "UTF-8");
//         for(int i =0;i<threads;++i){
//             StringBuilder tempString = new StringBuilder();
//             //int index=(int)((runFinishTime- runStartTime)/1000_000_000)+5;
//             for(int j=0;j<RT_TIME_SIZE;++j){
//                 tempString.append(threadTpsVec[i][j]+" ");
//             }
//             oswTT.write(tempString.toString());
//             oswTT.write("\n");
//         }
//         oswTT.close();

        //写tps文件
//         String tpsFilePath = "TPSResult"+"-"+threads+"-"+commandStr+"-"+baseSize+".txt";
//         File fileT = new File(tpsFilePath);
//         FileOutputStream fosT = null;
//         if(!fileT.exists()){
//             fileT.createNewFile();//如果文件不存在，就创建该文件
//             fosT = new FileOutputStream(fileT);//首次写入获取
//         }else{
//             //如果文件已存在，那么就在文件末尾追加写入
//             fosT = new FileOutputStream(fileT,true);//这里构造方法多了一个参数true,表示在文件末尾追加写入
//         }
//         OutputStreamWriter oswT = new OutputStreamWriter(fosT, "UTF-8");
        StringBuilder tempString = new StringBuilder();
        //int index=(int)((runFinishTime- runStartTime)/1000_000_000)+5;
        for(int i=0;i<RT_TIME_SIZE;++i){
            tempString.append(tpsVec[i]+" ");
        }
        System.out.println(tempString);
//         oswT.write(tempString.toString());
//         oswT.close();

        //写result
//         String filePath = "result"+"-"+commandStr+"-"+baseSize+".txt";
//         File file = new File(filePath);
//         FileOutputStream fos = null;
//         if(!file.exists()){
//             file.createNewFile();//如果文件不存在，就创建该文件
//             fos = new FileOutputStream(file);//首次写入获取
//         }else{
//             //如果文件已存在，那么就在文件末尾追加写入
//             fos = new FileOutputStream(file,true);//这里构造方法多了一个参数true,表示在文件末尾追加写入
//         }
//         OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
//         osw.write(threads+","+avgTps+","+avgRT+","+tp90+","+tp99+","+tp999+","+errorCount+","+ timeoutCount+","+outBucketCount+","+totalCnt.get()+","+maxTps+"\n");
//         for(int i=0;i<4;++i){
//             osw.write(threads+","+avgRTVec[i]+","+otp90[i]+","+otp99[i]+","+otp999[i]+","+rtBucketsVec[i][RT_BUCKET_SIZE-1].get()+"\n");
//         }
//         osw.close();
    }

    /**
     * run benchmark
     */
}

