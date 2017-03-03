package com.lxc.rbmq.client.consumer;

import com.lxc.rbmq.client.factory.ConnectionPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

/**
 * 消息队列消费者
 * 使用者通过继承此类，指定key即可消费指定队列
 * Created by chenlx
 * on 2016/8/24.
 */
public abstract class DefaultMessageConsumer extends SimpleMessageListenerContainer implements MessageListener{

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageConsumer.class);

    @Override
    public void onMessage(Message message) {
        logger.info("receive message:{}", message);
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return ConnectionPoolFactory.getConnectionFactory(getKey());
    }

    /**
     * conf-mq.properties文件中配置的key
     * @return
     */
    abstract String getKey();

    /**
     * 消息确认
     * @return
     */
    abstract boolean isAutoAck();

}
