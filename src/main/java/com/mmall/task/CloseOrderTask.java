package com.mmall.task;

import com.mmall.common.Const;
import com.mmall.service.IOrderService;
import com.mmall.util.PropertiesUtil;
import com.mmall.util.RedisShardedPoolUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

/**
 * Created by taoshiliu on 2018/2/6.
 */
@Component
@Slf4j
public class CloseOrderTask {

    @Autowired
    private IOrderService iOrderService;

    @Scheduled(cron="0 */1 * * * ?")
    public void closeOrderTaskV1(){
        log.info("Scheduled start");
        int hour = Integer.parseInt(PropertiesUtil.getProperty("close.order.task.time.hour","2"));
        iOrderService.closeOrder(hour);
        log.info("Scheduled end");
    }

    /*分布式关闭订单
    * setnx获取锁
    *   1、获得-加expire时间-执行业务逻辑-删除key（释放锁）
    *   0、未获得锁
    * ***存在的问题：未释放锁之前，系统down了，就会发生死锁
    * */
    public void closeOrderTaskV2(){

        long lockTimeout = Long.parseLong(PropertiesUtil.getProperty("lock.timeout","5000"));
        Long setnxResult = RedisShardedPoolUtil.setnx(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK,String.valueOf(System.currentTimeMillis()+lockTimeout));
        if(setnxResult != null &&setnxResult.intValue() == 1) {
            closeOrder(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
        }else {
            log.info("没有获得分布式锁:{}",Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
        }

    }

    private void closeOrder(String lockName) {
        RedisShardedPoolUtil.expire(lockName,50);
        log.info("获取{},ThreadName:{}",Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK,Thread.currentThread().getName());
        int hour = Integer.parseInt(PropertiesUtil.getProperty("close.order.task.time.hour","2"));
        iOrderService.closeOrder(hour);
        RedisShardedPoolUtil.del(lockName);
        log.info("释放{},ThreadName:{}",Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK,Thread.currentThread().getName());
        log.info("=======================================");
    }

    /*
    * 解决分布式方案一的方法
    * tomcat使用shutdown.sh（不是kill -9），会执行@PreDestory标签
    * ***存在的问题：需要写很多释放锁的方法
    * */
    @PreDestroy
    public void delLock() {
        RedisShardedPoolUtil.del(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
    }

    /*
    *分布式关闭订单
    * setnx获取锁key：KEY -> value: currentTime + timeout
    *   返回
    *   1、获得-加expire时间-执行业务逻辑-删除key（释放锁）
    *   0、未能获得锁，进一步判断
    *       a、若currentTime < KEY.value(currentTime + timeout) ,那个该锁一定是处于被锁状态
    *       b、若currentTime > KEY.value(currentTime + timeout) ,进一步判断，使用Redis的getset，获取KEY的原值O（currentTime + timeout），并设置key:KEY -> value: N(currentTime + timeout)
    *           ba、若O不存在或者O=N，表示KEY被del了，或者集群中无其他进程修改KEY
    *           bb、集群中存在其他进程修改了该KEY，无法获得该锁
    * ***分布式锁双重防死锁（很高级）
    * */
    public void closeOrderTaskV3(){
        long lockTimeout = Long.parseLong(PropertiesUtil.getProperty("lock.timeout","5000"));
        Long setnxResult = RedisShardedPoolUtil.setnx(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK,String.valueOf(System.currentTimeMillis()+lockTimeout));

        if(setnxResult != null &&setnxResult.intValue() == 1) {
            closeOrder(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
        }else {
            /*
            * 未获取锁的判断
            * */
            String lockValueStr = RedisShardedPoolUtil.get(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
            if(lockValueStr != null && System.currentTimeMillis() > Long.parseLong(lockValueStr)) {
                String getSetResult = RedisShardedPoolUtil.getSet(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK,String.valueOf(System.currentTimeMillis()+lockTimeout));
                /*
                * 集群环境下，极为重要，防止其他集群中的进程执行了该方法，修改了该key
                * */
                if(getSetResult == null || (getSetResult != null && StringUtils.equals(getSetResult,lockValueStr))) {
                    closeOrder(Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
                }else {
                    log.info("没有获得分布式锁:{}",Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
                }
            }else {
                log.info("没有获得分布式锁:{}",Const.REDIS_LOCK.CLOSE_ORDER_TASK_LOCK);
            }
        }

    }
}
