package com.jonas.jedis;

import lombok.Data;

@Data
public class YamlConfig {

    private RedisConfig redis;

    @Data
    public static class RedisConfig {
        private String host;
        private Integer port;
        private Integer maxIdle;
        private Integer maxTotal;
        private Integer maxWaitMillis;
    }
}
