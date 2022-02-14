package com.jonas.jedis;

import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class RedisAPI {
    private static int db = 0;
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

    public static void setDb(int db) {
        RedisAPI.db = db;
    }

    public static String lock(String key, int tryTime) {
        try {
            for (int i = 0; i < tryTime; i++) {
                String lockId = tryLock(key);
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

    public static String tryLock(String key) {
        try (Jedis jedis = getJedis()) {
            //锁id（必须拥有此id才能释放锁）
            String lockId = UUID.randomUUID().toString();
            String isSuccess = jedis.set(key, lockId, "NX", "PX", 5000);
            if ("OK".equalsIgnoreCase(isSuccess)) {
                return lockId;
            }
            return null;
        }
    }

    public static void unlock(String key, String lockId) {
        try (Jedis jedis = getJedis()) {
            //执行Lua代码删除lockId匹配的锁, Lua脚本具体原子性
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            jedis.eval(script, Collections.singletonList(key), Collections.singletonList(lockId));
        }
    }

    public static void zadd(String key, String member, int score) {
        try (Jedis jedis = getJedis()) {
            jedis.zadd(key, score, member);
        }
    }

    public static void zincrby(String key, String member, int score) {
        try (Jedis jedis = getJedis()) {
            jedis.zincrby(key, score, member);
        }
    }

    public static void zincrby(String key, Map<String /* member */, Integer /* score */> args) {
        try (Jedis jedis = getJedis()) {
            for (Map.Entry<String, Integer> entry : args.entrySet()) {
                jedis.zincrby(key, entry.getValue(), entry.getKey());
            }
        }
    }

    public static Set<String> zrange(String key, int len) {
        try (Jedis jedis = getJedis()) {
            return jedis.zrange(key, 0, len - 1);
        }
    }

    public static int zscore(String key, String member) {
        try (Jedis jedis = getJedis()) {
            Double score = jedis.zscore(key, member);
            return null != score ? score.intValue() : 0;
        }
    }

    /**
     * 获得key里面包含几个member
     *
     * @param key 键
     * @return member个数
     */
    public static int zcard(String key) {
        try (Jedis jedis = getJedis()) {
            return Math.toIntExact(jedis.zcard(key));
        }
    }

    public static void zremrangeByRank(String key, int start, int end) {
        try (Jedis jedis = getJedis()) {
            jedis.zremrangeByRank(key, start, end);
        }
    }

    public static void hset(String key, Map<String /* field */, String /* value */> args) {
        try (Jedis jedis = getJedis()) {
            for (Map.Entry<String, String> entry : args.entrySet()) {
                jedis.hset(key, entry.getKey(), entry.getValue());
            }
        }
    }

    public static String hget(String key, String field) {
        try (Jedis jedis = getJedis()) {
            return jedis.hget(key, field);
        }
    }

    public static Map<String, String> hgetAll(String key) {
        try (Jedis jedis = getJedis()) {
            return jedis.hgetAll(key);
        }
    }

    public static void sadd(String key, String... values) {
        try (Jedis jedis = getJedis()) {
            jedis.sadd(key, values);
        }
    }

    public static void srem(String key, String... values) {
        try (Jedis jedis = getJedis()) {
            jedis.srem(key, values);
        }
    }

    public static Set<String> smembers(String key) {
        try (Jedis jedis = getJedis()) {
            return jedis.smembers(key);
        }
    }

    public static long scard(String key) {
        try (Jedis jedis = getJedis()) {
            return jedis.scard(key);
        }
    }

    public static void set(String key, String value) {
        try (Jedis jedis = getJedis()) {
            jedis.set(key, value);
        }
    }

    public static void set(String key, String value, int expire) {
        try (Jedis jedis = getJedis()) {
            jedis.set(key, value);
            jedis.expire(key, expire);
        }
    }

    public static String get(String key) {
        try (Jedis jedis = getJedis()) {
            return jedis.get(key);
        }
    }

    public static void del(String key) {
        try (Jedis jedis = getJedis()) {
            jedis.del(key);
        }
    }

    /**
     * 批量获取符合patten模式的key
     *
     * @param pattern key模式，支持正则表达式
     * @return key集合
     */
    public static Set<String> scan(String pattern) {
        return scan(pattern, -1, 5000);
    }

    /**
     * 批量获取符合patten模式的key
     *
     * @param pattern key模式，支持正则表达式
     * @param count   非正数为不限制
     * @return key集合
     */
    public static Set<String> scan(String pattern, int count) {
        return scan(pattern, count, 5000);
    }

    /**
     * 批量获取符合patten模式的key
     *
     * @param pattern key模式，支持正则表达式
     * @param count   非正数为不限制
     * @param expire  超时，毫秒
     * @return key集合
     */
    public static Set<String> scan(String pattern, int count, long expire) {
        try (Jedis jedis = getJedis()) {
            // 游标初始值为0
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams();
            scanParams.match(pattern);
            if (0 < count) {
                scanParams.count(count);
            }

            Set<String> keys = new HashSet<>();

            //超过5秒立即中断
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start <= expire) {
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

    public static void pipeline(PipelineTask task) {
        try (Jedis jedis = getJedis()) {
            Pipeline pipeline = jedis.pipelined();
            task.doTask(pipeline);
            pipeline.sync();
        }
    }

    private static Jedis getJedis() {
        Jedis jedis = redisPool.getResource();
        if (db != jedis.getDB()) {
            jedis.select(db);
        }
        return jedis;
    }
}
