package com.fuqiang.springbootrocketmqconsumer.service;

/**
 * TODO //、、消息消费统一入口
 *
 * <p> Title: TopicTagService </p >
 * <p> Description: TopicTagService </p >
 * <p> History: 2020/12/18 11:37 </p >
 * <pre>
 *      Copyright (c) 2020 FQ (fuqiangvn@163.com) , ltd.
 * </pre>
 * Author  FQ
 * Version 0.0.1.RELEASE
 */
public interface TopicTagService {

    /**
     * @param message
     * @return void
     * @Description TODO    消息消费通知执行方法
     * @date 2020/12/18 14:54
     * @author Fuqiang
     */
    void execute(String message);
}
