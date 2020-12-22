package com.fuqiang.springbootrocketmqproducer.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * TODO //、、RocketMQ生产者配置
 *
 * <p> Title: RocketMQProducerConfig </p >
 * <p> Description: RocketMQProducerConfig </p >
 * <p> History: 2020/10/29 9:32 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@ConfigurationProperties(prefix = "rocketmq.producer")
@Component
@Data
@Slf4j
public class RocketMQProducerConfig {

    private String groupName;
    private String namesrvAddr;
    private Integer maxMessageSize;
    private Integer sendMsgTimeout;
    private Integer retryTimesWhenSendFailed;
    private Integer retryTimesWhenSendAsyncFailed;

    @Bean
    public DefaultMQProducer getRocketMQProducer() {

        DefaultMQProducer producer = new DefaultMQProducer(this.getGroupName());
        producer.setNamesrvAddr(this.getNamesrvAddr());
        //如果需要同一个jvm中不同的producer往不同的mq集群发送消息，需要设置不同的instanceName
        //producer.setInstanceName(instanceName);
        producer.setMaxMessageSize(this.getMaxMessageSize());
        producer.setSendMsgTimeout(this.getSendMsgTimeout());
        //如果发送消息失败，设置重试次数(同步和异步)，默认为2次
        producer.setRetryTimesWhenSendFailed(this.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(this.getRetryTimesWhenSendAsyncFailed());

        try {
            producer.start();
            log.info("====================【RocketMQ-producer启动成功，groupName：{}，namesrvAddr：{}】=====================", this.groupName, this.namesrvAddr);
        } catch (MQClientException e) {
            e.printStackTrace();
            log.error("=====================【RocketMQ-producer启动失败，原因：{}】=========================", e.getCause());
        }
        return producer;
    }

}
