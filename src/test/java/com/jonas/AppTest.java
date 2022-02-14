package com.jonas;

import com.jonas.jedis.RedisAPI;
import org.junit.Test;

import java.util.Random;
import java.util.Set;

public class AppTest {

    @Test
    public void testSet() {
        RedisAPI.set("test", "value", 1000);
    }

    @Test
    public void testZincrby() {
        RedisAPI.zincrby("zset", "A", 10);
        RedisAPI.zincrby("zset", "B", 20);
    }

    @Test
    public void testZadd() {
        RedisAPI.zadd("zset", "A", 10);
        RedisAPI.zadd("zset", "C", 20);
    }

    @Test
    public void testZrange() {
        Set<String> values = RedisAPI.zrange("zset", 2);
        System.out.println(values);
    }

    @Test
    public void testZscore() {
        int value = RedisAPI.zscore("zset", "B");
        System.out.println(value);
    }

    @Test
    public void testZcard() {
        int value = RedisAPI.zcard("zset1");
        System.out.println(value);
    }

    @Test
    public void testZremrangeByRank() {
        RedisAPI.zremrangeByRank("zset", 0, 0);
    }

    @Test
    public void testScoreboard() {
        String key = "zset";
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            int score = random.nextInt(100);
            RedisAPI.zadd(key, "v" + score, score);
            int nums = RedisAPI.zcard("zset");
            if (nums > 10) {
                RedisAPI.zremrangeByRank(key, 0, nums - 10);
            }
        }
    }

    @Test
    public void testPipeline() {
        RedisAPI.pipeline(pipeline -> {
            String key = "test:%s";
            for (int i = 0; i < 10; i++) {
                if (5 == i) {
                    throw new RuntimeException("运行时异常");
                }
                String k = String.format(key, i);
                pipeline.set(k, String.valueOf(i));
            }
        });
    }
}
