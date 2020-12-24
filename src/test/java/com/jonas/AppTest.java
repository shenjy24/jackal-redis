package com.jonas;

import com.jonas.jedis.RedisAPI;
import org.junit.Test;

public class AppTest {

    @Test
    public void testSet() {
        RedisAPI.set("test", "value", 1000, 0);
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
        }, 0);
    }
}
