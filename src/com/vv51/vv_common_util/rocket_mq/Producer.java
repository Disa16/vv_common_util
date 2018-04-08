package com.vv51.vv_common_util.rocket_mq;

import com.vv51.vv_common_util.other.ExceptionDump;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * Created by Kim on 2017/9/30.
 */
public class Producer {
    private static Logger logger = LogManager.getLogger(TransactionProducer.class);

    private String nameSvrAddr = "";
    private DefaultMQProducer defaultMQProducer = null;

    public Producer(String producerGroupName, String instanceName, String nameSvrAddr) {
        this.nameSvrAddr = nameSvrAddr;
        defaultMQProducer = new DefaultMQProducer(producerGroupName);
        defaultMQProducer.setInstanceName(instanceName);
    }

    public int start() {
        int ret = 0;
        defaultMQProducer.setNamesrvAddr(nameSvrAddr);
        try {
            defaultMQProducer.start();
            logger.error("DefaultMQProducer start success! nameSvrAddr:" + nameSvrAddr);
        } catch (Exception e) {
            logger.error("DefaultMQProducer start error[Exception]! nameSvrAddr:" + nameSvrAddr + " " + ExceptionDump.getErrorInfoFromException(e));
            ret = -100;
        }
        return ret;
    }

    public void stop() {
        defaultMQProducer.shutdown();
        logger.info("DefaultMQProducer Stop...");
    }

    public int sendMessage(String topic, String tag, String key, String body, long timeout) {
        int ret = 0;
        Message message = new Message(topic, tag, key, body.getBytes());
        try {
            SendResult sendResult;
            if (0 >= timeout) {
                sendResult = defaultMQProducer.send(message);
            } else {
                sendResult = defaultMQProducer.send(message, timeout);
            }
            if (SendStatus.SEND_OK != sendResult.getSendStatus()) {
                ret = -1;
                logger.error("DefaultMQProducer send message error[invalid SendStatus]! topic:" + topic + ", tag:" + tag + ", key:" + key + ", body:" + body + ", timeout:" + timeout + " | sendResult:" + sendResult);
            }
            logger.debug("DefaultMQProducer send message success! ");
        } /*catch (MQClientException e) {
            ret = -101;
        } catch (MQBrokerException e) {
            ret = -102;
        } catch (InterruptedException e) {
            ret = -103;
        }*/ catch (Exception e) {
            ret = -100;
            logger.error("DefaultMQProducer send message error[Exception]! topic:" + topic + ", tag:" + tag + ", key:" + key + ", body:" + body + ", timeout:" + timeout + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return ret;
    }

    public int sendMessageOrderly(String topic, String tag, String key, String body, int selectId, long timeout) {
        int ret = 0;
        Message message = new Message(topic, tag, key, body.getBytes());
        try {
            SendResult sendResult;
            if (0 >= timeout) {
                sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;
                        int index = id % list.size();
                        logger.debug("[" + System.currentTimeMillis() + "] Send message to queue id:" + id + ", list size:" + list.size() + ", index:" + index + ", key:" + key);
                        return list.get(index);
                    }
                }, selectId);
            } else {
                sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;
                        int index = id % list.size();
                        logger.debug("[" + System.currentTimeMillis() + "] Send message to queue id:" + id + ", list size:" + list.size() + ", index:" + index + ", key:" + key);
                        return list.get(index);
                    }
                }, selectId, timeout);
            }
            if (SendStatus.SEND_OK != sendResult.getSendStatus()) {
                ret = -1;
                logger.error("DefaultMQProducer send message error[invalid SendStatus]! topic:" + topic + ", tag:" + tag + ", key:" + key + ", body:" + body + ", timeout:" + timeout + " | sendResult:" + sendResult);
            }
            logger.debug("DefaultMQProducer send message success! ");
        } /*catch (MQClientException e) {
            ret = -101;
        } catch (MQBrokerException e) {
            ret = -102;
        } catch (InterruptedException e) {
            ret = -103;
        }*/ catch (Exception e) {
            ret = -100;
            logger.error("DefaultMQProducer send message error[Exception]! topic:" + topic + ", tag:" + tag + ", key:" + key + ", body:" + body + ", timeout:" + timeout + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return ret;
    }


    public static void main(String[] args) {
        Producer producer = new Producer("ProducerGroupName", "Producer", "182.118.27.83:9876");
        producer.start();
        long beginTime = System.currentTimeMillis();
        for (int i = 0; i < 101; ++i) {
            producer.sendMessageOrderly("Topic_NEW", "TAG", "[" + i + "] KEY", "" + i, i, 0);
        }
        System.out.println("USETIME:" + (System.currentTimeMillis() - beginTime));
        producer.stop();
    }
}
