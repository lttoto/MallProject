package com.mmall.task;

import com.mmall.service.IOrderService;
import com.mmall.util.PropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
}
