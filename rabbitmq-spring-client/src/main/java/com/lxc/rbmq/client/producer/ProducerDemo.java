package com.lxc.rbmq.client.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 原生的api调用示例
 * Created by chenlx
 * on 2016/8/24.
 */
public class ProducerDemo {

    /**
     * exchange有三种类型：
     * 1.direct：如果routing key匹配，那么message就会被投递到相应的queue
     * 2.fanout：会向响应的queueu广播
     * 3.topic：对key进行模式匹配，比如ab*可以传递到所有ab*的queue”
     */
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public Connection getConnection(){
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("");                // 设置IP
            factory.setPort(0);                 // 端口
            factory.setUsername("");            // 用户名
            factory.setPassword("");            // 密码
            factory.setConnectionTimeout(3000); // 连接超时时间

            return factory.newConnection();
        } catch (Exception e){
            logger.error("创建MQ连接异常！", e);
        }
        return null;
    }

    /**
     * 单发单收
     * @param queue
     * @param message
     * @throws Exception
     */
    public void sendMessageOneToOne(String queue, String message) throws Exception{
        Connection connection = getConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queue, false, false, false, null);
        // producer只能发送到exchange，它不能直接发送到queue；
        // 如果使用默认的exchange（名字是空字符串），这个默认的exchange允许我们发送给指定的queue
        channel.basicPublish("", queue, null, message.getBytes());

        channel.close();
        connection.close();
    }

    /**
     * 单发+持久化
     * @param queue
     * @param message
     * @throws Exception
     */
    public void sendMessageWithPersistent(String queue, String message) throws Exception{
        Connection connection = getConnection();
        Channel channel = connection.createChannel();
        // 使用消息持久化(第二个参数设置true)
        channel.queueDeclare(queue, true, false, false, null);
        channel.basicPublish("", queue, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

        channel.close();
        connection.close();
    }

    /**
     * 广播分发：发布/订阅式
     * @param exchange
     * @param message
     * @throws Exception
     */
    public void sendMessagePublishSubscribe(String exchange, String message) throws Exception{
        Connection connection = getConnection();
        Channel channel = connection.createChannel();
//        channel.exchangeDeclare(exchange, SendType.FANOUT.getValue());
        channel.exchangeDeclare(exchange, SendType.FANOUT.getValue(), true, false, false, null);
        channel.basicPublish(exchange, "", null, message.getBytes());
        channel.close();
        connection.close();
    }

    /**
     * 定向分发：routingKey完全匹配
     * @param exchange
     * @param routingKey
     * @param message
     * @throws Exception
     */
    public void sendMessageByRouting(String exchange, String routingKey, String message) throws Exception{
        Connection connection = getConnection();
        Channel channel = connection.createChannel();
        // 设置路由分发
        channel.exchangeDeclare(exchange, SendType.DIRECT.getValue());
        channel.basicPublish(exchange, routingKey, null, message.getBytes());
        channel.close();
        connection.close();
    }

    /**
     * 主题分发 routingKey匹配分发
     * @param exchange
     * @param routingKey
     * @param message
     * @throws Exception
     */
    public void sendMessageByRoutingKey(String exchange, String routingKey, String message) throws Exception{
        Connection connection = getConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(exchange, SendType.TOPIC.getValue());
        channel.basicPublish(exchange, routingKey, null, message.getBytes());
        channel.close();
        connection.close();
    }



    /**
     * 发送方式
     */
    enum SendType{

        TOPIC("topic"),
        FANOUT("fanout"),
        DIRECT("direct");

        private String value;

        public String getValue() {
            return value;
        }

        SendType(String value){
            this.value = value;
        }
    }
}
