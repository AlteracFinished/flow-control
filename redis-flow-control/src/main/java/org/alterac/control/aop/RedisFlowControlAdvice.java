package org.alterac.control.aop;

import org.alterac.control.annotation.FlowControl;
import org.alterac.control.exception.RequestOverflowException;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Aspect
@Component
public class RedisFlowControlAdvice {

    private Logger logger = LoggerFactory.getLogger(RedisFlowControlAdvice.class);

    private String header = "semaphore:";

    private String counter = "count:semaphore";

    @Resource
    private RedisTemplate<String,Long> redisTemplate;

    @Pointcut("@annotation(org.alterac.control.annotation.FlowControl)")
    public void flowControlMethod(){

    }

    @Before(value = "flowControlMethod()")
    public void flowControl(JoinPoint joinPoint){
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        FlowControl flowControl = methodSignature.getMethod().getAnnotation(FlowControl.class);
        String name = flowControl.name();
        long time = flowControl.time();
        if(time==0)return;
        if(name.equals("")){
            name=methodSignature.getName();
        }
        String key = header+name;
        long length = redisTemplate.boundZSetOps(key).size();
        if(length>time)throw new RequestOverflowException();

    }
}
