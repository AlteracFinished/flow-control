package org.alterac.control.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class RedisService {
    private Logger logger = LoggerFactory.getLogger(RedisService.class);

    private String header = "semaphore:";

    private String headerOwner = "semaphore:owner:";

    private String counter = "count:semaphore:";

    private String counterLock = "lock:count:";

    private int getCounterLockTimeout = 10;

    private int getSemaphoreTimeout = 10;

    private int counterLockTimeout = 10;

    @Resource
    private RedisTemplate<String,Long> redisTemplate;

    @Resource
    @Qualifier("lockRedisTemplate")
    private StringRedisTemplate stringRedisTemplate;

    public long getCountSize(){
        return redisTemplate.boundZSetOps(counter).size();
    }

    public String getCountLock(String name){
        String keyName = counterLock+name;
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND,getCounterLockTimeout);
        long end = calendar.getTimeInMillis();
        String key = UUID.randomUUID().toString();
        while(end > System.currentTimeMillis()){
            if(stringRedisTemplate.boundValueOps(keyName).setIfAbsent(key)){
                stringRedisTemplate.boundValueOps(keyName).expire(counterLockTimeout, TimeUnit.SECONDS);
                logger.info(stringRedisTemplate.boundValueOps(keyName).get());
                return key;
            }
            else if(stringRedisTemplate.boundValueOps(keyName).getExpire()==-1){
                stringRedisTemplate.boundValueOps(keyName).expire(counterLockTimeout, TimeUnit.SECONDS);
            }
            try {
                Thread.sleep(100L);
            }catch (InterruptedException e){
                logger.error("sleep failed!",e);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public boolean releaseCounterLock(String name,String lock){
        SessionCallback<List<Object>> sessionCallback = new SessionCallback<List<Object>>() {
            @Override
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                String counterLockName = counterLock+name;
                //operations.watch(counterLockName);
                /*Set<String> keys = operations.keys("*");
                for(String key:keys){
                    logger.info("key:"+key);
                }*/
                Object val = operations.boundValueOps(counterLockName).get();
                logger.info("name:"+counterLockName+" lock:"+ val);
                operations.multi();
                /*if(Arrays.equals(operations.boundValueOps(counterLockName).get(),lock.getBytes())){
                    connection.multi();
                    connection.del(counterLockBytes);
                    return connection.exec();
                }*/
                //operations.unwatch();
                return operations.exec();
            }
        };
        /*RedisCallback<List<Object>> callback = new RedisCallback<List<Object>>() {
            @Override
            public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
                byte[] counterLockBytes = (counterLock+name).getBytes();
                connection.openPipeline();
                connection.watch(counterLockBytes);
                byte[] val = connection.get(counterLockBytes);
                logger.info("lock:"+new String(val));
                if(Arrays.equals(counterLockBytes,lock.getBytes())){
                    connection.multi();
                    connection.del(counterLockBytes);
                    return connection.exec();
                }
                connection.unwatch();
                return new ArrayList<>();
            }
        };*/
        List m = stringRedisTemplate.executePipelined(sessionCallback);
        for(Object k:m){
            logger.info("item:"+k);
        }
        return m!=null&&m.size()>0;
    }

    public String getSemaphore(String name,long time){
        String key = UUID.randomUUID().toString();
        String semaphoreName = header + name;
        long now = System.currentTimeMillis();

        return null;
    }
}
