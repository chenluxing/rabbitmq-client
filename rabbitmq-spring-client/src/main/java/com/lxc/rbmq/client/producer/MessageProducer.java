package com.lxc.rbmq.client.producer;

import com.lxc.rbmq.client.factory.ConnectionPoolFactory;

/**
 * 消息推送
 * Created by chenlx
 * on 2016/11/1.
 */
public class MessageProducer {

    public static void sendMessage(String key, String message){
        ConnectionPoolFactory.sendMessage(key, message.getBytes());
    }

    public static void sendMessage(String key, byte[] message){
        ConnectionPoolFactory.sendMessage(key, message);
    }

}
