package com.demo;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 使用令牌桶算法实现限流
 */
public class LimitingDemo {

    private static final String redisAddress = "redis://xxx";
    private static RedisClient client = RedisClient.create(redisAddress);
    private static StatefulRedisConnection<String, String> connection = client.connect();
    private static RedisAsyncCommands async = connection.async();

    // 获取令牌脚本（1秒内最多请求3次）
    private static String luaScript =
            "local key = KEYS[1] " +
            "local limit = 3 " +
            "local times = tonumber(redis.call('get', key) or '0') + 1 " +
            "if times > limit then " +
            "    return 1 " +
            "elseif times == 1 then " +
            "    redis.call('set', key, times, 'PX', 1000) " +
            "    return 0 " +
            "else " +
            "    redis.call('INCRBY', key, '1') " +
            "    return 0 " +
            "end";

    public static void main(String[] args) {
        int threadCount = 5;
        CountDownLatch countdown = new CountDownLatch(threadCount);
        String userId = "u001";
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                service(userId);
                countdown.countDown();
            }).start();
        }
        try {
            countdown.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            client.shutdown();
        }
    }

    private static void service(String userId) {
        if(!acquire(userId)) {
            return;
        }
        System.out.println("service...");
    }

    /**
     * 获取令牌
     */
    private static boolean acquire(String userId) {
        boolean result = false;
        try {
            int retryCount = 0;
            while(true) {
                RedisFuture future = async.eval(luaScript, ScriptOutputType.INTEGER, userId);
                result = (long) future.get() == 0;
                if(result || ++retryCount >= 3) {
                    break;
                }
                Thread.sleep(400);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
/*
运行结果：
service...
service...
service...
service...
有一个请求被拦截。
 */