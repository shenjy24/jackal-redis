package com.jonas.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author shenjy
 * @date 2020/6/17
 * @description
 */
public class JedisManager {

    private static JedisPoolConfig jedisPoolConfig;
    public static JedisPool jedisPool;
    public static final Integer DB = 5;
    public static final String LOCK_KEY = "LOCK_KEY";
    public Random random = new Random();

    static {
        jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(Config.MAX_IDLE);
        jedisPoolConfig.setMaxTotal(Config.MAX_TOTAL);
        jedisPoolConfig.setMaxWaitMillis(Config.MAX_WAIT_MILLIS);
        //实例化连接池
        jedisPool = new JedisPool(jedisPoolConfig, Config.URL, Config.PORT);
    }

    public String lock(String key, int tryTime) {
        try {
            for (int i = 0; i < tryTime; i++) {
                String lockId = tryLock(key);
                if (null != lockId) {
                    return lockId;
                }
                TimeUnit.MILLISECONDS.sleep(random.nextInt(1000));
            }
        } catch (Exception e) {
        }
        return null;
    }

    public String tryLock(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(DB);

            //锁id（必须拥有此id才能释放锁）
            String lockId = UUID.randomUUID().toString();
            String isSuccess = jedis.set(key, lockId, "NX", "PX", 5000);
            if ("OK".equalsIgnoreCase(isSuccess)) {
                return lockId;
            }
            return null;
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public void unlock(String key, String lockId) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.select(DB);

            //执行Lua代码删除lockId匹配的锁, Lua脚本具体原子性
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            jedis.eval(script, Collections.singletonList(key), Collections.singletonList(lockId));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void delZSet(String key, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }
            jedis.del(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void setZSet(String key, Map<String /* member */, Integer /* score */> args, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }
            for (Map.Entry<String, Integer> entry : args.entrySet()) {
                jedis.zincrby(key, entry.getValue(), entry.getKey());
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static Set<String> zrange(String key, int len, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }
            return jedis.zrange(key, 0, len);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void setHash(String key, Map<String /* field */, String /* value */> args, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }

            for (Map.Entry<String, String> entry : args.entrySet()) {
                jedis.hset(key, entry.getKey(), entry.getValue());
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static String getHash(String key, String field, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }

            return jedis.hget(key, field);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void set(String key, String value, int expire, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }
            jedis.set(key, value);
            jedis.expire(key, expire);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static String get(String key, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }
            return jedis.get(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void del(String key, int db) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            if (db != jedis.getDB()) {
                jedis.select(db);
            }
            jedis.del(key);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void publish(String channel, String message) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.publish(channel, message);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    public static void subscribe(JedisPubSub jedisPubSub, String channel) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.subscribe(jedisPubSub, channel);
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }
}
