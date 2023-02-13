package com.jd.raft.benchmark;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.options.DeleteOption;

import java.util.concurrent.ExecutionException;

/**
 * Created by wujianchao on 2021/3/15.
 */
public class EtcdClientTest {

    public static final String ROOT_PATH = "/etcd_client_test";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        testDeleteOption();
        testClientConnection();
    }

    public static void testClientConnection(){ //测试链接，启动十个线程去泡
        for (int i = 0; i < 10; i++) {
            final int index = i;
            Thread thread = new Thread(() -> {
                KV etcd = createClient();
                ByteSequence id = ByteSequence.from((""+index).getBytes());
                try {
                    etcd.put(id, id).get();
                    System.out.println(index + " ready");
                    Thread.sleep(1000_000);
                    etcd.delete(id).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    etcd.close();
                }
            }, "worker-" + i);
            thread.start();
        }
    }

    public static void testDeleteOption() throws ExecutionException, InterruptedException {
        KV etcd = createClient();
        int workerId = 0;
        createdNodes(etcd, workerId);
        deleteCreatedNodes(etcd, workerId);
    }

    private static void createdNodes(KV etcd, int workerId) throws InterruptedException, ExecutionException {
        etcd.put(ByteSequence.from((ROOT_PATH + "/" + workerId).getBytes()), ByteSequence.from((""+workerId).getBytes())).get();
        for(int i=0;i<10;i++){
            String path = ROOT_PATH + "/" + workerId + "/" + i;
            ByteSequence key = ByteSequence.from(path.getBytes());
            etcd.put(key, ByteSequence.from((""+i).getBytes())).get();
        }

    }
    private static void deleteCreatedNodes(KV etcd, int workerId) throws InterruptedException, ExecutionException {
        String path = ROOT_PATH + "/" + workerId;
        ByteSequence key = ByteSequence.from(path.getBytes());
        DeleteOption option = DeleteOption.newBuilder().withPrefix(key).build();
        long deleted = etcd.delete(key, option).get().getDeleted();
        System.out.println("delete " + deleted + " nodes for workerId " + workerId);
    }

    public static KV createClient() {
        // init zookeeper client
        return Client.builder().endpoints("http://localhost:2379").build().getKVClient();
    }
}
