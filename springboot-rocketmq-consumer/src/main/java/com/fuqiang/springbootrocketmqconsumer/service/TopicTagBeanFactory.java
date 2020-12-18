package com.fuqiang.springbootrocketmqconsumer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO //
 *
 * <p> Title: TopicBeanFactory </p >
 * <p> Description: TopicBeanFactory </p >
 * <p> History: 2020/12/18 11:34 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
@Service
public class TopicTagBeanFactory {

    @Autowired
    private Map<String, TopicTagService> beans = new ConcurrentHashMap<>(2);

    /**
     * @param type beanName，如:pay_order
     * @return com.fuqiang.payserver.service.RedisKeyExpirationService
     * @Description TODO    根据type值，获取不同的处理对象bean
     * @date 2020/10/16 15:06
     * @author Fuqiang
     */
    public TopicTagService getInstance(String type) {
        TopicTagService handle = beans.get(type);
        if (handle == null) {
            throw new RuntimeException("类型暂不支持");
        }
        return handle;
    }
}
