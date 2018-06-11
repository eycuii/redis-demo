package com.demo;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

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

    // 获取令牌脚本
    private static String luaScript =
            "local key = KEYS[1] " +
            "local limit = 3 " + // 1秒内最多请求3次
            "local current = tonumber(redis.call('get', key) or '0') " +
            "if current + 1 > limit then " +
            "    return 0 " +
            "else " +
            "    redis.call('INCRBY', key, '1') " +
            "    redis.call('expire', key, '1') " + // 1秒后重新计数
            "    return 1 " +
            "end";

    public static void main(String[] args) {
        String userId = "u001";
        for (int i = 0; i < 10; i++) {
            service(userId);
        }
        connection.close();
        client.shutdown();
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
        RedisFuture future = async.eval(luaScript, ScriptOutputType.INTEGER, userId);
        try {
            result = ((long) future.get()) == 1;
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
 */

