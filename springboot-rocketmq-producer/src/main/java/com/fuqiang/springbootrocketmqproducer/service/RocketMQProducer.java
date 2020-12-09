package com.fuqiang.springbootrocketmqproducer.service;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * TODO //、、自定义RocketMQ模板方法
 *
 * <p> Title: RocketMQProducer </p >
 * <p> Description: RocketMQProducer </p >
 * <p> History: 2020/10/29 14:49 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@Component
@Slf4j
public class RocketMQProducer {

    @Autowired
    private DefaultMQProducer defaultMQProducer;

    /**
     * TODO 单条消息发送（同步）
     * 发送消息采用同步模式，这种方式只有在消息完全发送完成之后才返回结果，此方式存在需要同步等待发送结果的时间代价
     */
    public <T> boolean syncSend(String topic, String tag, T message) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        log.info("sync发送单条消息：{}", message.toString());
        Message var = new Message(topic, tag, message.toString().getBytes());
        SendResult sendResult = defaultMQProducer.send(var);
        boolean flag = sendResult.getSendStatus().equals(SendStatus.SEND_OK);

        if (flag) {
            log.info("sync单条消息发送成功，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        } else {
            log.error("sync单条消息发送失败，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        }
        return flag;
    }

    /**
     * TODO 批量消息发送（同步）
     * 发送消息采用同步模式，这种方式只有在消息完全发送完成之后才返回结果，此方式存在需要同步等待发送结果的时间代价
     */
    public <T> boolean syncSend(String topic, String tag, List<T> messages) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        log.info("sync发送批量消息：{}", messages);
        Message var = new Message(topic, tag, messages.toString().getBytes());
        SendResult sendResult = defaultMQProducer.send(var);
        boolean flag = sendResult.getSendStatus().equals(SendStatus.SEND_OK);

        if (flag) {
            log.info("sync批量消息发送成功，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        } else {
            log.error("sync批量消息发送失败，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        }
        return flag;
    }

    /**
     * TODO 单条消息发送，设置超时时间（同步）
     * 发送消息采用同步模式，这种方式只有在消息完全发送完成之后才返回结果，此方式存在需要同步等待发送结果的时间代价
     * <p>
     * timeout：超时时间，单位毫秒
     * </p>
     */
    public <T> boolean syncSend(String topic, String tag, T message, long timeout) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        log.info("sync发送单条消息：{}，超时时间：{}ms", message, timeout);
        Message var = new Message(topic, tag, message.toString().getBytes());
        SendResult sendResult = defaultMQProducer.send(var, timeout);
        boolean flag = sendResult.getSendStatus().equals(SendStatus.SEND_OK);

        if (flag) {
            log.info("sync单条消息发送成功，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        } else {
            log.error("sync单条消息发送失败，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        }
        return flag;
    }

    /**
     * TODO 批量消息发送，设置超时时间（同步）
     * 发送消息采用同步模式，这种方式只有在消息完全发送完成之后才返回结果，此方式存在需要同步等待发送结果的时间代价
     * <p>
     * timeout：超时时间，单位毫秒
     * </p>
     */
    public <T> boolean syncSend(String topic, String tag, List<T> messages, long timeout) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        log.info("sync发送批量消息：{}，超时时间：{}ms", messages, timeout);
        Message var = new Message(topic, tag, messages.toString().getBytes());
        SendResult sendResult = defaultMQProducer.send(var, timeout);
        boolean flag = sendResult.getSendStatus().equals(SendStatus.SEND_OK);

        if (flag) {
            log.info("sync批量消息发送成功，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        } else {
            log.error("sync批量消息发送失败，消息ID：{}，topic：{}，brokerName：{}", sendResult.getMsgId(), sendResult.getMessageQueue().getTopic(), sendResult.getMessageQueue().getBrokerName());
        }
        return flag;
    }

    /**
     * TODO 单条消息发送（异步）
     * 发送消息采用异步发送模式，消息发送后立刻返回，当消息完全完成发送后，会调用回调函数sendCallback来告知发送者本次发送是成功或者失败
     * 异步模式通常用于响应时间敏感业务场景，即承受不了同步发送消息时等待返回的耗时代价
     * <p>
     * 可更改当前方法，SendResult作为动态参数传递，onSuccess和onException做业务逻辑处理
     * </p>
     */
    public <T> void asyncSend(String topic, String tag, T message) throws RemotingException, MQClientException, InterruptedException {

        log.info("async发送单条消息：{}", message);
        Message var = new Message(topic, tag, message.toString().getBytes());
        defaultMQProducer.send(var, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("async异步回调成功，回调结果：{}", JSON.toJSONString(sendResult));
            }

            @Override
            public void onException(Throwable throwable) {
                log.error("async异步回调失败，原因：{}", throwable.getCause().toString());
            }
        });
    }

    /**
     * TODO 单条消息发送，自定义SendCallback（异步）
     * 发送消息采用异步发送模式，消息发送后立刻返回，当消息完全完成发送后，会调用回调函数sendCallback来告知发送者本次发送是成功或者失败
     * 异步模式通常用于响应时间敏感业务场景，即承受不了同步发送消息时等待返回的耗时代价
     */
    public <T> void asyncSend(String topic, String tag, T message, SendCallback sendCallback) throws RemotingException, MQClientException, InterruptedException {

        log.info("async发送单条消息：{}", message);
        Message var = new Message(topic, tag, message.toString().getBytes());
        defaultMQProducer.send(var, sendCallback);
    }

    /**
     * TODO 单条消息发送，设置超时时间（异步）
     * 发送消息采用异步发送模式，消息发送后立刻返回，当消息完全完成发送后，会调用回调函数sendCallback来告知发送者本次发送是成功或者失败
     * 异步模式通常用于响应时间敏感业务场景，即承受不了同步发送消息时等待返回的耗时代价
     * <p>
     * 可更改当前方法，SendResult作为动态参数传递，onSuccess和onException做业务逻辑处理
     * </p>
     */
    public <T> void asyncSend(String topic, String tag, T message, long timeout) throws RemotingException, MQClientException, InterruptedException {

        log.info("async发送单条消息：{}，超时时间：{}ms", message, timeout);
        Message var = new Message(topic, tag, message.toString().getBytes());
        defaultMQProducer.send(var, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("async异步回调成功，回调结果：{}", JSON.toJSONString(sendResult));
            }

            @Override
            public void onException(Throwable throwable) {
                log.error("async异步回调失败，原因：{}", throwable.getCause().toString());
            }
        }, timeout);
    }

    /**
     * TODO 单条消息发送，设置超时时间，自定义SendCallback（异步）
     * 发送消息采用异步发送模式，消息发送后立刻返回，当消息完全完成发送后，会调用回调函数sendCallback来告知发送者本次发送是成功或者失败
     * 异步模式通常用于响应时间敏感业务场景，即承受不了同步发送消息时等待返回的耗时代价
     */
    public <T> void asyncSend(String topic, String tag, T message, SendCallback sendCallback, long timeout) throws RemotingException, MQClientException, InterruptedException {

        log.info("async发送单条消息：{}，超时时间：{}ms", message, timeout);
        Message var = new Message(topic, tag, message.toString().getBytes());
        defaultMQProducer.send(var, sendCallback, timeout);
    }

}
