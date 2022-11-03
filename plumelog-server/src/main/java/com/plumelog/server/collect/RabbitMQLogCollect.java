package com.plumelog.server.collect;

import com.plumelog.core.client.AbstractClient;
import com.plumelog.core.constant.LogMessageConstant;
import com.plumelog.core.rabbit.RabbitMQClient;
import com.plumelog.server.client.ElasticLowerClient;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * className：KafkaLogCollect
 * description：KafkaLogCollect 获取kafka中日志，存储到es
 *
 * @author Frank.chen
 * @version 1.0.0
 */
public class RabbitMQLogCollect extends BaseLogCollect {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(RabbitMQLogCollect.class);
    private AbstractClient redisClient;
    private RabbitMQClient rabbitMQClient;

    public RabbitMQLogCollect(ElasticLowerClient elasticLowerClient, RabbitMQClient rabbitMQClient, ApplicationEventPublisher applicationEventPublisher, AbstractClient redisClient) {
        super.elasticLowerClient = elasticLowerClient;
        this.rabbitMQClient = rabbitMQClient;
        super.applicationEventPublisher = applicationEventPublisher;
        super.redisClient = redisClient;
        logger.info("sending log ready!");
    }

    public void rabbitmqStart() {
        threadPoolExecutor.execute(() -> {
            collectRuningLog();
        });
        logger.info("RabbitLogCollect is starting!");
    }

    public void collectRuningLog() {
//        while (true) {
            List<String> logList = new ArrayList();
            List<String> sendlogList = new ArrayList();

            try {
                Channel channel = rabbitMQClient.getChannel();
                Channel traceChannel = rabbitMQClient.getTraceChannel();

                DefaultConsumer consumer = new DefaultConsumer(channel) {
                    //监听的queue中有消息进来时，会自动调用此方法来处理消息。但此方法默认是空的，需要重写
                    @Override
                    public void handleDelivery(java.lang.String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                        java.lang.String msg = new java.lang.String(body);
//                        System.out.println("received msg: " + msg);
                        save(asList(msg));
                    }
                };
                DefaultConsumer traceConsumer = new DefaultConsumer(traceChannel) {
                    //监听的queue中有消息进来时，会自动调用此方法来处理消息。但此方法默认是空的，需要重写
                    @Override
                    public void handleDelivery(java.lang.String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                        java.lang.String msg = new java.lang.String(body);
//                        System.out.println("received msg: " + msg);
                        saveTrace(asList(msg));
                    }
                };

                channel.basicConsume(LogMessageConstant.LOG_KEY, true, consumer);
                traceChannel.basicConsume(LogMessageConstant.LOG_KEY_TRACE, true, traceConsumer);
            } catch (Exception e) {
                logger.error("get logs from rabbit failed! ", e);
            }


//            if (logList.size() > 0) {
//                super.sendLog(super.getRunLogIndex(), logList);
//
//                publisherMonitorEvent(logList);
//            }
//            if (sendlogList.size() > 0) {
//                super.sendTraceLogList(super.getTraceLogIndex(), sendlogList);
//            }
//        }
    }

    private void save(List<String> logList){
        if (logList.size() > 0) {
            super.sendLog(super.getRunLogIndex(), logList);

            publisherMonitorEvent(logList);
        }
    }
    private void saveTrace(List<String> logList){
        if (logList.size() > 0) {
            super.sendLog(super.getRunLogIndex(), logList);

            publisherMonitorEvent(logList);
        }
    }
}
