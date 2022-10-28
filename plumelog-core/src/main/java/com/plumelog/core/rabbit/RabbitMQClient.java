package com.plumelog.core.rabbit;

import com.plumelog.core.client.AbstractClient;
import com.plumelog.core.constant.LogMessageConstant;
import com.plumelog.core.exception.LogQueueConnectException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RabbitMQClient extends AbstractClient {

    private static RabbitMQClient instance;

    private ConnectionFactory connectionFactory;

    private Channel channel;


    public RabbitMQClient(String host, int port, String userName, String password) {
        try{
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(host);
            connectionFactory.setPort(port);
            connectionFactory.setUsername(userName);
            connectionFactory.setPassword(password);
            connectionFactory.setVirtualHost("/");

        } catch (Exception e){
            System.out.println("rabbitmq连接失败");
            System.out.println(e);
        }

    }

    public static RabbitMQClient getInstance(String host, int port, String userName, String password) {
        if (instance == null) {
            synchronized (RabbitMQClient.class) {
                if (instance == null) {
                    instance = new RabbitMQClient(host, port, userName, password);
                }
            }
        }
        return instance;
    }


    @Override
    public void pushMessage(String key, String strings) throws LogQueueConnectException {
        try {
            send(strings, LogMessageConstant.LOG_DEAD_EXCHANGE, LogMessageConstant.LOG_ROUT_KEY);
        } catch (Exception e) {
            throw new LogQueueConnectException("MQ 写入失败！", e);
        } finally {

        }
    }


    @Override
    public void putMessageList(String key, List<String> list) throws LogQueueConnectException {
        try {
            list.forEach(str -> {
                try {
                    send(str, LogMessageConstant.LOG_DEAD_EXCHANGE, LogMessageConstant.LOG_ROUT_KEY);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            throw new LogQueueConnectException("MQ 写入失败！", e);
        } finally {

        }
    }


    private void send(String content, String exchangeName, String rootKey) throws Exception {
        String correlationId = generateCorrelationId();

        //由连接工厂创建连接
        Connection connection = connectionFactory.newConnection();
        //通过连接创建信道
        channel = connection.createChannel();

        //通过信道声明一个exchange，若已存在则直接使用，不存在会自动创建
        //参数：name、type、是否支持持久化、此交换机没有绑定一个queue时是否自动删除、是否只在rabbitmq内部使用此交换机、此交换机的其它参数（map）
        channel.exchangeDeclare(LogMessageConstant.LOG_EXCHANGE, "topic", true, false, false, null);

        //通过信道声明一个queue，如果此队列已存在，则直接使用；如果不存在，会自动创建
        //参数：name、是否支持持久化、是否是排它的、是否支持自动删除、其他参数（map）
        channel.queueDeclare(LogMessageConstant.LOG_QUEUE, true, false, false, null);

        //将queue绑定至某个exchange。一个exchange可以绑定多个queue
        channel.queueBind(LogMessageConstant.LOG_QUEUE, LogMessageConstant.LOG_EXCHANGE, LogMessageConstant.LOG_ROUT_KEY);

        //发送message
        channel.basicPublish(LogMessageConstant.LOG_EXCHANGE, LogMessageConstant.LOG_ROUT_KEY, null, content.getBytes());
    }

    private static String generateCorrelationId() {
        Date date =new Date();
        long time = date.getTime();
        return UUID.randomUUID().toString().replaceAll("-", "") + time;
    }

}
