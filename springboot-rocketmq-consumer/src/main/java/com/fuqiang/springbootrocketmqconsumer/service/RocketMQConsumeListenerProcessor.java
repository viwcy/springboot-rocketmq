package com.fuqiang.springbootrocketmqconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * TODO //、、RocketMQ监听器
 *
 * <p> Title: MQConsumeMsgListenerProcessor </p >
 * <p> Description: MQConsumeMsgListenerProcessor </p >
 * <p> History: 2020/10/29 10:08 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@Component
@Slf4j
public class RocketMQConsumeListenerProcessor implements MessageListenerConcurrently {

    /**
     * 默认List<MessageExt>里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息<br/>
     * 不要抛异常，如果没有return CONSUME_SUCCESS ，consumer会重新消费该消息，直到return CONSUME_SUCCESS
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messageExtList, ConsumeConcurrentlyContext context) {

        if (CollectionUtils.isEmpty(messageExtList)) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        MessageExt messageExt = messageExtList.get(0);
        String message = new String(messageExt.getBody());
        log.info("RocketMQ接收到消息：{}", message);

        //TODO 判断该消息是否重复消费（RocketMQ不保证消息不重复，如果你的业务需要保证严格的不重复消息，需要你自己在业务端去重）
        //TODO 获取该消息重试次数
        int reconsume = messageExt.getReconsumeTimes();
        //消息已经重试了3次，如果不需要再次消费，则返回成功，做额外的补偿机制，自行扩展
        if (reconsume == 3) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
        //TODO 策略模式处理对应的业务逻辑
        try {
            System.out.println("打印消息：" + message);
            log.info("消息处理完毕...");
            //TODO 存储消息表，做幂等性处理
        } catch (Exception e) {
            e.printStackTrace();
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        // 如果没有return success ，consumer会重新消费该消息，直到return success
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
