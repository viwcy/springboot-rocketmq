package com.fuqiang.springbootrocketmqconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * TODO //、、顺序消息监听器
 *
 * <p> Title: OrderlyConsumeListenerProcessor </p >
 * <p> Description: OrderlyConsumeListenerProcessor </p >
 * <p> History: 2020/12/25 14:47 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@Component
@Slf4j
public class OrderlyConsumeListenerProcessor implements MessageListenerOrderly {

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
        MessageQueue messageQueue = consumeOrderlyContext.getMessageQueue();
        try {
            log.info("监听到顺序消息，topic：{}，queueId：{}，数据：{}", messageQueue.getTopic(), messageQueue.getQueueId(), new String(list.get(0).getBody()));
            //TODO  判断重试消费次数，做补偿机制
            if (list.get(0).getReconsumeTimes() >= 3) {
                log.info("重试消费3次结束...数据：{}", new String(list.get(0).getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }
            //TODO  正常消费逻辑，进行幂等处理，防止重复消费
            System.out.println("消费顺序消息：" + new String(list.get(0).getBody()));
            return ConsumeOrderlyStatus.SUCCESS;
        } catch (Exception e) {
            log.info("顺序消息消费失败，topic：{}，queueId：{}，数据：{}", messageQueue.getTopic(), messageQueue.getQueueId(), new String(list.get(0).getBody()));
            e.printStackTrace();
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }
}
