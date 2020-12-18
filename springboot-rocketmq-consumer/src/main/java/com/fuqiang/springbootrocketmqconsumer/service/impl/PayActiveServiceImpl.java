package com.fuqiang.springbootrocketmqconsumer.service.impl;

import com.fuqiang.springbootrocketmqconsumer.service.TopicTagService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * TODO //
 *
 * <p> Title: PayActiveServiceImpl </p >
 * <p> Description: PayActiveServiceImpl </p >
 * <p> History: 2020/12/18 11:45 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@Service("pay-topic-active")
@Slf4j
public class PayActiveServiceImpl implements TopicTagService {

    @Override
    public void execute(String message) {
        System.out.println("pay-topic-active消息消费，内容：" + message);
    }
}
