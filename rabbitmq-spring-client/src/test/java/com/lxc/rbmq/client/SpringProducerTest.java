package com.lxc.rbmq.client;

import com.lxc.rbmq.client.factory.ConnectionPoolFactory;

/**
 * @auth chenlx
 */
public class SpringProducerTest {

    public static void main(String[] args) {
        String message = "这个是一条rabbitmq的测试消息2222";
//        ConnectionPoolFactory.sendMessage("mq.test", message.getBytes());

//        ConnectionPoolFactory.sendMessage("mq.fanout", message.getBytes());

        ConnectionPoolFactory.sendTopicMessage("mq.topic", message.getBytes());
        System.out.println("消息发送完成。。。");
    }

}
