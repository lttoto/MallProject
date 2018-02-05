package com.mmall.util;


import com.mmall.common.RedisSharededPool;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

/**
 * Created by taoshiliu on 2018/2/5.
 */
@Slf4j
public class RedisShardedPoolUtil {

    public static Long expire(String key,int exTime) {
        ShardedJedis jedis = null;
        Long result = null;

        try {
            jedis = RedisSharededPool.getJedis();
            result = jedis.expire(key,exTime);
        } catch (Exception e) {
            log.error("setex key:{} error",key,e);
            RedisSharededPool.returnBrokenResource(jedis);
            return result;
        }

        RedisSharededPool.returnResource(jedis);
        return result;
    }

    public static String setEx(String key,String value,int exTime) {
        ShardedJedis jedis = null;
        String result = null;

        try {
            jedis = RedisSharededPool.getJedis();
            result = jedis.setex(key,exTime,value);
        } catch (Exception e) {
            log.error("setex key:{} value:{} error",key,value,e);
            RedisSharededPool.returnBrokenResource(jedis);
            return result;
        }

        RedisSharededPool.returnResource(jedis);
        return result;
    }

    public static String set(String key,String value) {
        ShardedJedis jedis = null;
        String result = null;

        try {
            jedis = RedisSharededPool.getJedis();
            result = jedis.set(key, value);
        } catch (Exception e) {
            log.error("set key:{} value:{} error",key,value,e);
            RedisSharededPool.returnBrokenResource(jedis);
            return result;
        }

        RedisSharededPool.returnResource(jedis);
        return result;
    }

    public static String get(String key) {
        ShardedJedis jedis = null;
        String result = null;

        try {
            jedis = RedisSharededPool.getJedis();
            result = jedis.get(key);
        } catch (Exception e) {
            log.error("get key:{} error",key,e);
            RedisSharededPool.returnBrokenResource(jedis);
            return result;
        }

        RedisSharededPool.returnResource(jedis);
        return result;
    }

    public static Long del(String key) {
        ShardedJedis jedis = null;
        Long result = null;

        try {
            jedis = RedisSharededPool.getJedis();
            result = jedis.del(key);
        } catch (Exception e) {
            log.error("get key:{} error",key,e);
            RedisSharededPool.returnBrokenResource(jedis);
            return result;
        }

        RedisSharededPool.returnResource(jedis);
        return result;
    }

    public static void main(String[] args) {
        ShardedJedis jedis = RedisSharededPool.getJedis();

        RedisShardedPoolUtil.set("testKey","testValue");
        String value = RedisShardedPoolUtil.get("testKey");
        RedisShardedPoolUtil.setEx("keyex","valueex",60*10);
        RedisShardedPoolUtil.expire("testKey",60*20);
        RedisShardedPoolUtil.del("testKey");

        System.out.println("End");
    }

}
