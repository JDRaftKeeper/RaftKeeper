package com.jd.raft.benchmark;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.options.DeleteOption;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wujianchao on 2021/3/2.
 */
public class EtcdTask implements Runnable {

    private int id;
    private KV etcd;

    private CountDownLatch latch;

    public EtcdTask(CountDownLatch latch, int id){
        this.latch = latch;
        this.id = id;
    }

    public static KV createClient() {
        // init zookeeper client
        return Client.builder().endpoints("http://" + Main.client).build().getKVClient();
    }

    public void run() {

        latch.countDown();
    }

}
