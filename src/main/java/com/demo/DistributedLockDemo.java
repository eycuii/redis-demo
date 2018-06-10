package com.demo;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisStringCommands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class DistributedLockDemo {

    private static final String redisAddress = "redis://xxx";
    private static final String lockKey = "countLock";

    // 如果不存在时获取锁，并设置过期时间（毫秒）
    private static final SetArgs lockArgs = new SetArgs().px(1000).nx();
    // 延长1000毫秒
    private static final SetArgs delayArgs = new SetArgs().px(1000).xx();

    private static int count;

    public static void main(String[] args) {
        try {
            concurrencyTestWithDistributedLock(10, 100);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void concurrencyTestWithDistributedLock(int threadCount, int sleepMillis) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch countdown = new CountDownLatch(threadCount);

        initCount();
        long start = System.currentTimeMillis();

        for(int i = 0; i < threadCount; i++) {
            RedisClient client = RedisClient.create(redisAddress);
            StatefulRedisConnection<String, String> connection = client.connect();
            RedisStringCommands sync = connection.sync();
            String threadId = "thread-" + i;
            executorService.execute(new Thread(() -> {
                int retryCount = 0;
                Thread daemonTread = createDaemonThread(threadId, sync);
                try {
                    while(true) {
                        if("OK".equals(sync.set(lockKey, threadId, lockArgs))) {
                            System.out.println(threadId + "获取锁");
                            daemonTread.start();
                            increaseCount(threadId);
                            break;
                        }
                        retryCount++;
                        Thread.sleep(sleepMillis);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    daemonTread.interrupt();
                    deleteLockSafely(threadId, connection);
                    System.out.println(threadId + "释放了锁 retryCount:" + retryCount);
                    connection.close();
                    client.shutdown();
                    countdown.countDown();
                }
            }));
        }

        countdown.await();
        long end = System.currentTimeMillis();
        executorService.shutdown();
        System.out.println("count:" + count
                + " threadCount:" + threadCount
                + " sleepMillis:" + sleepMillis
                + " costTime:" + (end-start));
    }

    private static void initCount() {
        RedisClient client = RedisClient.create(redisAddress);
        StatefulRedisConnection<String, String> connection = client.connect();
        connection.async().del(lockKey);
        connection.close();
        client.shutdown();

        count = 0;
    }

    private static void increaseCount(String threadId) throws Exception {
        for(int i = 0; i < 10000; i++) {
            count++;
            if(i % 1000 == 0) {
                Thread.sleep(120);
            }
        }
        System.out.println(threadId + " ---------- new count::" + count);
    }

    /**
     * （原子操作）如果是当前线程，则删除(/释放)锁
     */
    private static void deleteLockSafely(String threadId,
                                        StatefulRedisConnection<String, String> connection) {
        String luaScript = "if redis.call('get', KEYS[1]) == KEYS[2] then " +
                "return redis.call('del', KEYS[1]) " +
                "else return 0 end";
        connection.async().eval(luaScript, ScriptOutputType.INTEGER, lockKey, threadId);
    }

    /**
     * 守护线程：如果用户线程还没执行完，则延长过期时间。
     */
    private static Thread createDaemonThread(String threadId, RedisStringCommands sync) {
        Thread daemonTread = new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(800);
                    sync.set(lockKey, threadId, delayArgs);
                    System.out.println(threadId + ".....daemon");
                }
            } catch (InterruptedException | RedisCommandInterruptedException e) {
//                e.printStackTrace();
            }
        });
        daemonTread.setDaemon(true);
        return daemonTread;
    }
}
/*
threadCount = 10，sleepMillis = 100 时：
thread-0获取锁
thread-0.....daemon
thread-0 ---------- new count::10000
thread-0释放了锁 retryCount:0
thread-4获取锁
thread-4.....daemon
thread-4 ---------- new count::20000
thread-4释放了锁 retryCount:6
thread-5获取锁
thread-5.....daemon
thread-5 ---------- new count::30000
thread-5释放了锁 retryCount:13
thread-3获取锁
thread-3.....daemon
thread-3 ---------- new count::40000
thread-3释放了锁 retryCount:23
thread-6获取锁
thread-6.....daemon
thread-6 ---------- new count::50000
thread-6释放了锁 retryCount:32
thread-1获取锁
thread-1.....daemon
thread-1 ---------- new count::60000
thread-1释放了锁 retryCount:43
thread-8获取锁
thread-8.....daemon
thread-8 ---------- new count::70000
thread-8释放了锁 retryCount:45
thread-2获取锁
thread-2.....daemon
thread-2 ---------- new count::80000
thread-2释放了锁 retryCount:60
thread-7获取锁
thread-7.....daemon
thread-7 ---------- new count::90000
thread-7释放了锁 retryCount:63
thread-9获取锁
thread-9.....daemon
thread-9 ---------- new count::100000
thread-9释放了锁 retryCount:73
count:100000 threadCount:10 sleepMillis:100 costTime:13488
 */