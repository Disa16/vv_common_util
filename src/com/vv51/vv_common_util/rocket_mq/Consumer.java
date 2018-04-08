package com.vv51.vv_common_util.rocket_mq;

import com.vv51.vv_common_util.other.ExceptionDump;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by Kim on 2017/9/29.
 */
public class Consumer {
    private static Logger logger = LogManager.getLogger(Consumer.class);
    private String name = "";
    private String nameSvrAddr = "";
    private String topic = "";
    private String tags = "*";
    private String consumerGroup = "";

    private DefaultMQPushConsumer defaultMQPushConsumer = null;

    public Consumer(String name, String nameSvr, String topic, String tags, String consumerGroup) {
        this.name = name;
        this.nameSvrAddr = nameSvr;
        this.topic = topic;
        if (null != tags && !tags.equals("")) {
            this.tags = tags;
        }
        this.consumerGroup = consumerGroup;
        defaultMQPushConsumer = new DefaultMQPushConsumer(name);
    }

    public int start(MessageHandler messageHandler, int batchMaxSize) {
        int ret = 0;
        try {
            defaultMQPushConsumer.setNamesrvAddr(nameSvrAddr);
            defaultMQPushConsumer.subscribe(topic, tags);
            if (!consumerGroup.equals("")) {
                defaultMQPushConsumer.setConsumerGroup(consumerGroup);
            }
            defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            if (0 < batchMaxSize) {
                defaultMQPushConsumer.setConsumeMessageBatchMaxSize(batchMaxSize);
            }
            defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext
                        consumeConcurrentlyContext) {
                    try {
                        for (int i = 0; i < list.size(); i++) {
                            MessageExt message = list.get(i);
                            messageHandler.handle(message);
                            logger.debug("DefaultMQPushConsumer[" + name + "] consume message[" + i + "]:" + message);
                        }
                    } catch (Exception e) {
                        logger.error("DefaultMQPushConsumer consume message error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
//            System.out.println(defaultMQPushConsumer.toString());
            defaultMQPushConsumer.start();
            logger.info("DefaultMQPushConsumer[" + name + "] start success! name:" + name + ", nameSvrAddr:" + nameSvrAddr + ", topic:" + topic + ", tags:" + tags + ", consumerGroup:" + consumerGroup + " " + defaultMQPushConsumer.toString());
        } catch (Exception e) {
            logger.error("DefaultMQPushConsumer start error[Exception]! name:" + name + ", nameSvrAddr:" + nameSvrAddr + ", topic:" + topic + ", tags:" + tags + ", consumerGroup:" + consumerGroup + " " + ExceptionDump.getErrorInfoFromException(e));
            ret = -100;
        }
        return ret;
    }

    public void stop() {
        defaultMQPushConsumer.shutdown();
    }
}
