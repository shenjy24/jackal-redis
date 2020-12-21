package com.jonas;

import com.jonas.jedis.RedisUtils;
import org.junit.Test;

public class AppTest {

    @Test
    public void testSet() {
        RedisUtils.set("test", "value", 1000, 0);
    }
}
