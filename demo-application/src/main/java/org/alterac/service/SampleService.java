package org.alterac.service;


import org.alterac.control.annotation.FlowControl;
import org.alterac.control.redis.RedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;

@Service
public class SampleService {
    private Logger logger = LoggerFactory.getLogger(SampleService.class);

    @Resource
    private RedisService redisService;

    @FlowControl(time = 5)
    public void printDate(){
        logger.info(new Date().toString());
    }

    public String lock(){
        String key = redisService.getCountLock("sample");
        redisService.releaseCounterLock("sample",key);
        return key;
    }
}
