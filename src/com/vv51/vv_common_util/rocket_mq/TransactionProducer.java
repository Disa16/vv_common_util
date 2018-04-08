package com.vv51.vv_common_util.rocket_mq;

import com.vv51.vv_common_util.other.ExceptionDump;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created by Kim on 2017/9/29.
 */
public class TransactionProducer {
    private static Logger logger = LogManager.getLogger(TransactionProducer.class);

    private String nameSvrAddr = "";
    private TransactionMQProducer transactionMQProducer = null;

    public TransactionProducer(String producerGroupName, String instanceName, String nameSvrAddr) {
        this.nameSvrAddr = nameSvrAddr;
        this.transactionMQProducer = new TransactionMQProducer(producerGroupName);
        this.transactionMQProducer.setInstanceName(instanceName);
    }

    public int start(TransactionCheckListener transactionCheckListener, LocalTransactionExecuter localTransactionExecuter) {
        int ret = 0;
        this.transactionMQProducer.setNamesrvAddr(nameSvrAddr);
        try {
            transactionMQProducer.start();
            transactionMQProducer.setTransactionCheckListener(transactionCheckListener);
            logger.error("TransactionMQProducer start success! nameSvrAddr:" + nameSvrAddr + ", transactionCheckListener:" + transactionCheckListener);
        } catch (Exception e) {
            logger.error("TransactionMQProducer start error[Exception]! nameSvrAddr:" + nameSvrAddr + ", transactionCheckListener:" + transactionCheckListener + " " + ExceptionDump.getErrorInfoFromException(e));
            ret = -100;
        }
        return ret;
    }

    public void stop() {
        transactionMQProducer.shutdown();
        logger.info("TransactionMQProducer Stop...");
    }

    public int sendMessage(String topic, String tag, String key, String body, LocalTransactionExecuter localTransactionExecuter, String args) {
        int ret = 0;
        Message message = new Message(topic, tag, key, body.getBytes());
        try {
            SendResult sendResult = transactionMQProducer.sendMessageInTransaction(message, localTransactionExecuter, args);
            if (SendStatus.SEND_OK != sendResult.getSendStatus()) {
                ret = -1;
                logger.error("TransactionMQProducer send message error[invalid SendStatus]! topic:" + topic + ", tag:" + tag + ", key:" + key + ", body:" + body + " | sendResult:" + sendResult);
            }
            logger.debug("TransactionMQProducer send message success! topic:" + topic + ", tag:" + tag + ", key:" + key + ", body:" + body);
        }  catch (Exception e) {
            ret = -100;
            logger.error("TransactionMQProducer send message error[Exception]! topic:" + topic + ", tag:" + tag + ", key:" + key + ", body:" + body + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return ret;
    }
}