package com.fuqiang.springbootrocketmqconsumer.config;

import com.fuqiang.springbootrocketmqconsumer.service.OrderlyConsumeListenerProcessor;
import com.fuqiang.springbootrocketmqconsumer.service.RocketMQConsumeListenerProcessor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * TODO //、、RocketMQ消费者配置
 *
 * <p> Title: RocketMQConsumer </p >
 * <p> Description: RocketMQConsumer </p >
 * <p> History: 2020/10/29 9:36 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@ConfigurationProperties(prefix = "rocketmq.consumer")
@Component
@Slf4j
@Data
public class RocketMQConsumerConfig {

    private String namesrvAddr;
    private String groupName;
    private Integer consumeThreadMin;
    private Integer consumeThreadMax;
    private String topics;
    private Integer consumeMessageBatchMaxSize;

    @Autowired
    private RocketMQConsumeListenerProcessor rocketMQConsumeListenerProcessor;

    @Autowired
    private OrderlyConsumeListenerProcessor orderlyConsumeListenerProcessor;

    @Bean
    public DefaultMQPushConsumer getRocketMQConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(this.getGroupName());
        consumer.setNamesrvAddr(this.getNamesrvAddr());
        consumer.setConsumeThreadMin(this.getConsumeThreadMin());
        consumer.setConsumeThreadMax(this.getConsumeThreadMax());
        consumer.registerMessageListener(orderlyConsumeListenerProcessor);
        //设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
        //如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //设置消费模型，集群还是广播，默认为集群。广播能保证至少一次消费，但是消费失败后不做重试
        consumer.setMessageModel(MessageModel.CLUSTERING);
        //设置一次消费消息的条数，默认为1条
        consumer.setConsumeMessageBatchMaxSize(this.getConsumeMessageBatchMaxSize());
        try {
            //设置该消费者订阅的主题和tag，如果是订阅该主题下的所有tag，则tag使用*；如果需要指定订阅该主题下的某些tag，则使用||分割，例如tag1||tag2||tag3
            String[] topicTagsArr = topics.split(";");
            for (String topicTags : topicTagsArr) {
                String[] topicTag = topicTags.split("~");
                consumer.subscribe(topicTag[0], topicTag[1]);
            }
            consumer.start();
            log.info("=====================【RocketMQ-consumer启动成功，groupName：{}，namesrvAddr：{}】======================", this.groupName, this.namesrvAddr);
        } catch (MQClientException e) {
            e.printStackTrace();
            log.error("======================【RocketMQ-consumer启动失败，原因：{}=====================】", e.getCause());
        }
        return consumer;
    }

}
