package com.lxc.rbmq.client.factory;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.util.Assert;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by chenlx
 * on 2016/8/26.
 */
public class ConnectionPoolFactory {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolFactory.class);

    private static String CONFIG_HOST = "mq.host";
    private static String CONFIG_PORT = "mq.port";
    private static String CONFIG_FILE_NAME = "conf-mq.properties";

    private static String host;
    private static int port;
    private static Properties properties = null;
    private static volatile Map<String, ConnectionConfiguration> pools = new HashMap<String, ConnectionConfiguration>();

    // 类加载时，初始化配置文件信息
    static {
        InputStream in = null;
        try {
            properties = new Properties();
            in = ClassLoader.getSystemResourceAsStream(CONFIG_FILE_NAME);
            properties.load(in);
            host = String.valueOf(properties.get(CONFIG_HOST));
            port = Integer.valueOf(String.valueOf(properties.get(CONFIG_PORT)));
        } catch (Exception e) {
            logger.error("加载mq配置文件异常！", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 发送消息
     *
     * @param key
     * @param msg
     */
    public static void sendMessage(String key, byte[] msg) {
        Assert.notNull(key, "mq配置Key不允许为空");
        Assert.notNull(msg, "消息体不允许为空");
        Message message = new Message(msg, new MessageProperties());
        getTemplate(key).send(message);
    }

    /**
     * 发送消息
     *
     * @param key
     * @param msg
     */
    public static void sendTopicMessage(String key, byte[] msg) {
        Assert.notNull(key, "mq配置Key不允许为空");
        Assert.notNull(msg, "消息体不允许为空");
        Message message = new Message(msg, new MessageProperties());
        getTemplate(key).convertAndSend(message);
    }

    /**
     * 获取连接
     *
     * @param key
     * @return
     */
    public static ConnectionFactory getConnectionFactory(String key) {
        return getTemplate(key).getConnectionFactory();
    }

    private static RabbitTemplate getTemplate(String key) {
        if (!pools.containsKey(key) && pools.get(key) == null) {
            createConnectionFactory(key);
        }
        return pools.get(key).getTemplate();
    }

    /**
     * 根据Key值，读取配置文件，创建对应的connectionFactory
     *
     * @param key
     * @return
     */
    synchronized private static void createConnectionFactory(String key) {
        if (properties != null) {
            try {
                String value = String.valueOf(properties.get(key));
                ConnectionConfiguration.ConnectionProperties prop = JSONObject.parseObject(value, ConnectionConfiguration.ConnectionProperties.class);
                if (prop != null) {
                    pools.put(key, new ConnectionConfiguration(createConnectionFactory(prop), prop));
                }
            } catch (Exception e) {
                logger.error("获取key:{}的mq连接异常！", key, e);
            }
        }
    }

    /**
     * 根据属性创建连接
     *
     * @param prop
     * @return
     */
    private static org.springframework.amqp.rabbit.connection.ConnectionFactory createConnectionFactory(ConnectionConfiguration.ConnectionProperties prop) {
        CachingConnectionFactory factory = new CachingConnectionFactory(host, port);
        factory.setUsername(prop.getUsername());
        factory.setPassword(prop.getPassword());
        factory.setVirtualHost(prop.getVhost());
        factory.setConnectionTimeout(3000);     // 连接超时时间
        return factory;
    }

}
