package com.fuqiang.springbootrocketmqproducer.controller;

import com.fuqiang.springbootrocketmqproducer.service.RocketMQProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * TODO //
 *
 * <p> Title: ProducerController </p >
 * <p> Description: ProducerController </p >
 * <p> History: 2020/12/3 17:01 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@RestController
@RequestMapping("/producer")
public class ProducerController {

    @Autowired
    private RocketMQProducer rocketMQProducer;

    @PostMapping("/send")
    public String send(@RequestParam("message") String message) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        boolean send = rocketMQProducer.syncSend("test-topic", "test1", message);
        return send ? "OK" : "FAIL";
    }
}
