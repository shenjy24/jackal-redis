package com.jonas.jedis;

import lombok.SneakyThrows;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class RedisAPI {
    private static final Pool<Jedis> redisPool;
    private static final Random random = new Random();

    static {
        YamlConfig yamlConfig = YamlParser.toBean("config.yml", YamlConfig.class);
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(yamlConfig.getRedis().getMaxIdle());
        jedisPoolConfig.setMaxTotal(yamlConfig.getRedis().getMaxTotal());
        jedisPoolConfig.setMaxWaitMillis(yamlConfig.getRedis().getMaxWaitMillis());

        redisPool = new JedisPool(jedisPoolConfig, yamlConfig.getRedis().getHost(), yamlConfig.getRedis().getPort());
    }

    public static String lock(String key, int tryTime, int db) {
        try {
            for (int i = 0; i < tryTime; i++) {
                String lockId = tryLock(key, db);
                if (null != lockId) {
                    return lockId;
                }
                TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String tryLock(String key, int db) {
        try (Jedis jedis = getJedis(db)) {
            //锁id（必须拥有此id才能释放锁）
            String lockId = UUID.randomUUID().toString();
            String isSuccess = jedis.set(key, lockId, "NX", "PX", 5000);
            if ("OK".equalsIgnoreCase(isSuccess)) {
                return lockId;
            }
            return null;
        }
    }

    public static void unlock(String key, String lockId, int db) {
        try (Jedis jedis = getJedis(db)) {
            //执行Lua代码删除lockId匹配的锁, Lua脚本具体原子性
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            jedis.eval(script, Collections.singletonList(key), Collections.singletonList(lockId));
        }
    }

    public static void delZSet(String key, int db) {
        try (Jedis jedis = getJedis(db)) {
            jedis.del(key);
        }
    }

    public static void setZSet(String key, Map<String /* member */, Integer /* score */> args, int db) {
        try (Jedis jedis = getJedis(db)) {
            for (Map.Entry<String, Integer> entry : args.entrySet()) {
                jedis.zincrby(key, entry.getValue(), entry.getKey());
            }
        }
    }

    public static Set<String> getZSet(String key, int len, int db) {
        try (Jedis jedis = getJedis(db)) {
            return jedis.zrange(key, 0, len);
        }
    }

    public static void setHash(String key, Map<String /* field */, String /* value */> args, int db) {
        try (Jedis jedis = getJedis(db)) {
            for (Map.Entry<String, String> entry : args.entrySet()) {
                jedis.hset(key, entry.getKey(), entry.getValue());
            }
        }
    }

    public static String getHash(String key, String field, int db) {
        try (Jedis jedis = getJedis(db)) {
            return jedis.hget(key, field);
        }
    }

    public static Map<String, String> getHashAll(String key, int db) {
        try (Jedis jedis = getJedis(db)) {
            return jedis.hgetAll(key);
        }
    }

    public static void set(String key, String value, int expire, int db) {
        try (Jedis jedis = getJedis(db)) {
            jedis.set(key, value);
            jedis.expire(key, expire);
        }
    }

    public static String get(String key, int db) {
        try (Jedis jedis = getJedis(db)) {
            return jedis.get(key);
        }
    }

    public static void del(String key, int db) {
        try (Jedis jedis = getJedis(db)) {
            jedis.del(key);
        }
    }

    public static Set<String> scan(String pattern, int count, int db) {
        try (Jedis jedis = getJedis(db)) {
            // 游标初始值为0
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams();
            scanParams.match(pattern);
            scanParams.count(count);

            Set<String> keys = new HashSet<>();

            //超过5秒立即中断
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start <= 5000) {
                //使用scan命令获取数据，使用cursor游标记录位置，下次循环使用
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getStringCursor();
                keys.addAll(scanResult.getResult());
                //当服务器返回值为0的游标时， 表示迭代已结束
                if (ScanParams.SCAN_POINTER_START.equals(cursor)) {
                    break;
                }
            }
            return keys;
        }
    }

    public static void publish(String channel, String message) {
        try (Jedis jedis = getJedis()) {
            jedis.publish(channel, message);
        }
    }

    public static void subscribe(JedisPubSub jedisPubSub, String channel) {
        try (Jedis jedis = getJedis()) {
            jedis.subscribe(jedisPubSub, channel);
        }
    }

    public static void pipeline(PipelineTask task, int db) {
        try (Jedis jedis = getJedis(db)) {
            Pipeline pipeline = jedis.pipelined();
            task.doTask(pipeline);
            pipeline.sync();
        }
    }

    private static Jedis getJedis() {
        return getJedis(0);
    }

    private static Jedis getJedis(int db) {
        Jedis jedis = redisPool.getResource();
        if (db != jedis.getDB()) {
            jedis.select(db);
        }
        return jedis;
    }
}
