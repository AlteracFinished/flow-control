package org.alterac.control.redis;

import org.alterac.control.exception.RequestOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class RedisService {
    private Logger logger = LoggerFactory.getLogger(RedisService.class);

    private String header = "semaphore:";

    private String headerOwner = "semaphore:owner:";

    private String counter = "semaphore:counter:";

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


    public boolean releaseCounterLock(String name,String lock){
        if(lock==null)return false;
        String counterLockName = counterLock+name;
        String val;
        SessionCallback<List<Object>> sessionCallback = new ReleaseLockCallback(counterLockName);
        while(true){
            Object m=null;
            val = stringRedisTemplate.boundValueOps(counterLockName).get();
            if(lock.equals(val)){
                m = stringRedisTemplate.executePipelined(sessionCallback);
            }else {
                return false;
            }
            if(m!=null)return true;
        }
    }

    public String getSemaphore(String name,Long time,Long limit){
        String lock = getCountLock(name);
        if(lock!=null&&!lock.equals("")){
            String semaphore = stringRedisTemplate.execute(new AcquireSemaphoreCallback(name,5L));
            if(semaphore!=null)return semaphore;
            else throw new RequestOverflowException("Request to "+name+" over limit!");
        }
        return null;
    }

    public boolean releaseSemaphore(String name,String lock){
        return stringRedisTemplate.execute(new ReleaseSemaphoreCallback(name,lock));
    }

    private class ReleaseLockCallback implements SessionCallback{

        private String lockName;

        public ReleaseLockCallback(String lockName){
            this.lockName=lockName;
        }

        @Override
        public Object execute(RedisOperations redisOperations) throws DataAccessException {
            redisOperations.watch(lockName);
            redisOperations.multi();
            redisOperations.delete(lockName);
            return redisOperations.exec();
        }
    }

    private class AcquireSemaphoreCallback implements RedisCallback<String>{

        private String name;

        private Long limit;

        public AcquireSemaphoreCallback(String name,Long limit){
            this.name=name;
            this.limit=limit;
        }

        @Override
        public String doInRedis(RedisConnection connection) throws DataAccessException {
            String key = UUID.randomUUID().toString();
            String semaphoreName = header + name;
            String ownerName = headerOwner + name;
            String counterName = counter + name;
            long now = System.currentTimeMillis();
            connection.openPipeline();
            RedisSerializer<String> serializer = stringRedisTemplate.getStringSerializer();
            connection.zRemRangeByScore(serializer.serialize(semaphoreName),Double.MIN_VALUE,now-getSemaphoreTimeout*1000);
            connection.zInterStore(serializer.serialize(ownerName), RedisZSetCommands.Aggregate.SUM,new int[]{1,0},serializer.serialize(ownerName),serializer.serialize(semaphoreName));

            connection.incr(serializer.serialize(counterName));
            Long cnt = (Long)connection.closePipeline().get(2);

            connection.openPipeline();

            connection.zAdd(serializer.serialize(semaphoreName),now,serializer.serialize(key));
            connection.zAdd(serializer.serialize(ownerName),cnt,serializer.serialize(key));

            connection.zRank(serializer.serialize(ownerName),serializer.serialize(key));

            if(limit > (Long)connection.closePipeline().get(2)){
                return key;
            }

            connection.openPipeline();

            connection.zRem(serializer.serialize(semaphoreName),serializer.serialize(key));
            connection.zRem(serializer.serialize(ownerName),serializer.serialize(key));
            connection.closePipeline();
            return null;
        }
    }

    private class ReleaseSemaphoreCallback implements RedisCallback<Boolean>{

        private String name;

        private String lock;

        public ReleaseSemaphoreCallback(String name,String lock){
            this.name=name;
            this.lock=lock;
        }

        @Override
        public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
            connection.openPipeline();
            String headerName = header+name;
            String ownerName = headerOwner+name;
            RedisSerializer<String> serializer = stringRedisTemplate.getStringSerializer();
            connection.zRem(serializer.serialize(headerName),serializer.serialize(lock));
            connection.zRem(serializer.serialize(ownerName),serializer.serialize(lock));
            return ((Long)connection.closePipeline().get(0))==1;
        }
    }
}
