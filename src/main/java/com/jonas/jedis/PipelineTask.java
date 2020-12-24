package com.jonas.jedis;

import redis.clients.jedis.Pipeline;

public interface PipelineTask {
    void doTask(Pipeline pipeline);
}
