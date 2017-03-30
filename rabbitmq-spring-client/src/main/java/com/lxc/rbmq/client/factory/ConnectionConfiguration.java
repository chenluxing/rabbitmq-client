package com.lxc.rbmq.client.factory;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * 连接配置信息
 * Created by chenlx
 * on 2016/11/1.
 */
public class ConnectionConfiguration {

    private ConnectionFactory factory;
    private ConnectionProperties properties;
    private RabbitTemplate template;

    public ConnectionConfiguration(ConnectionFactory factory, ConnectionProperties properties) {
        this.factory = factory;
        this.properties = properties;
        RabbitAdmin admin = new RabbitAdmin(factory);
        Exchange exchange = null;
        if (ExchangeType.FANOUT.getValue().equals(properties.getType())) {
            // 广播/订阅分发 不需要绑定routingkey，直接对exchange下所有队列进行广播
            exchange = new FanoutExchange(properties.getExchange());
            admin.declareExchange(exchange);
            template = admin.getRabbitTemplate();
            template.setExchange(properties.getExchange());
        } else if (ExchangeType.TOPIC.getValue().equals(properties.getType())) {
            // 主题分发：路由表达式匹配
            exchange = new TopicExchange(properties.getExchange());
            admin.declareExchange(exchange);
            template = admin.getRabbitTemplate();
            template.setExchange(properties.getExchange());
            template.setRoutingKey(properties.getRoutingkey());
        } else if (ExchangeType.DIRECT.getValue().equals(properties.getType())){
            // 定向分发：路由完全匹配
            exchange = new DirectExchange(properties.getExchange());
            admin.declareExchange(exchange);
            template = admin.getRabbitTemplate();
            template.setExchange(properties.getExchange());
            template.setRoutingKey(properties.getRoutingkey());
        } else {
            Queue queue = new Queue(properties.getQueue());
            admin.declareQueue(queue);
            template = admin.getRabbitTemplate();
        }
    }

    public RabbitTemplate getTemplate(){
        return this.template;
    }

    public ConnectionFactory getFactory() {
        return factory;
    }

    /**
     * 连接配置信息
     */
    public static class ConnectionProperties {

        private String vhost;
        private String exchange;
        private String username;
        private String password;
        private String routingkey;
        private String queue;
        private String type;

        public String getVhost() {
            return vhost;
        }

        public void setVhost(String vhost) {
            this.vhost = vhost;
        }

        public String getExchange() {
            return exchange;
        }

        public void setExchange(String exchange) {
            this.exchange = exchange;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getRoutingkey() {
            return routingkey;
        }

        public void setRoutingkey(String routingkey) {
            this.routingkey = routingkey;
        }

        public String getQueue() {
            return queue;
        }

        public void setQueue(String queue) {
            this.queue = queue;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    enum ExchangeType {
        DIRECT("direct"),
        TOPIC("topic"),
        FANOUT("fanout");

        private String value;

        ExchangeType(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }
    }

}
