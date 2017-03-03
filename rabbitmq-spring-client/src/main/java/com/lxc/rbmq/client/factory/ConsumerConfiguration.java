package com.lxc.rbmq.client.factory;

import com.rabbitmq.client.AMQP;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;

/**
 * Created by chenlx
 * on 2016/8/26.
 */
public class ConsumerConfiguration {

    public ConnectionFactory getConnectionFactory(){

        CachingConnectionFactory connectionFactory = new CachingConnectionFactory("ip");
        connectionFactory.setUsername("username");
        connectionFactory.setPassword("password");
        connectionFactory.setPort(AMQP.PROTOCOL.PORT);

        return connectionFactory;
    }

    public RabbitTemplate getRabbitTemplate(){
        RabbitTemplate template = new RabbitTemplate(getConnectionFactory());
        template.setRoutingKey("");
        template.setQueue("");
        return template;
    }

    public SimpleMessageListenerContainer getListenerContainer(){
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(getConnectionFactory());
        container.setMessageListener(new MessageListenerAdapter(new ReceiveMsgHandler()));
        return container;
    }

    class ReceiveMsgHandler{
        public void handleMessage(String text) {
            System.out.println("Received: " + text);
        }
    }
}