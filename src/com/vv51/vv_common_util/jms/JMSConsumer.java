package com.vv51.vv_common_util.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;



import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vv51.vv_common_util.other.ExceptionDump;


/**
 * JMS消息消费者
 *
 */
public class JMSConsumer implements ExceptionListener {
    private static Logger logger = LogManager.getLogger(JMSConsumer.class);

    public final static int DEFAULT_QUEUE_PREFETCH = 10;

    //队列预取策略
    private int queuePrefetch = DEFAULT_QUEUE_PREFETCH;

    private String mBrokerUrl;
    private String mUserName;
    private String mPassword;

    private MessageListener mMessageListener;
    private ActiveMQConnection mConnection;
    private Session mSession;
    //队列名
    private String mQueueName;

    /**
     * 执行消息获取的操作
     * @throws Exception
     */
    public void start() throws Exception {
        //ActiveMQ的连接工厂
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(this.mUserName, this.mPassword, this.mBrokerUrl);
        mConnection = (ActiveMQConnection) activeMQConnectionFactory.createConnection();
        //activeMQ预取策略
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setQueuePrefetch(queuePrefetch);
        ((ActiveMQConnection) mConnection).setPrefetchPolicy(prefetchPolicy);
        mConnection.setExceptionListener(this);
        //会话采用非事务级别，消息到达机制使用自动通知机制
        mSession = mConnection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
        Destination destination = mSession.createQueue(this.mQueueName);
        MessageConsumer consumer = mSession.createConsumer(destination);
        consumer.setMessageListener(this.mMessageListener);
        mConnection.start();
    }


    /**
     * 关闭连接
     */
    public void shutdown(){
        try {
            if (null != mSession) {
                mSession.close();
                mSession=null;
            }
            if (null != mConnection) {
                mConnection.close();
                mConnection=null;
            }
        } catch (Exception e) {
            logger.error("JMSConsumer shutdown error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }

    @Override
    public void onException(JMSException e) {
        e.printStackTrace();
    }


    public String getBrokerUrl() {
        return mBrokerUrl;
    }


    public void setBrokerUrl(String brokerUrl) {
        this.mBrokerUrl = brokerUrl;
    }


    public String getUserName() {
        return mUserName;
    }


    public void setUserName(String userName) {
        this.mUserName = userName;
    }


    public String getPassword() {
        return mPassword;
    }


    public void setPassword(String password) {
        this.mPassword = password;
    }


    public String getQueue() {
        return mQueueName;
    }


    public void setQueue(String queue) {
        this.mQueueName = queue;
    }


    public MessageListener getMessageListener() {
        return mMessageListener;
    }


    public void setMessageListener(MessageListener messageListener) {
        this.mMessageListener = messageListener;
    }


    public int getQueuePrefetch() {
        return queuePrefetch;
    }


    public void setQueuePrefetch(int queuePrefetch) {
        this.queuePrefetch = queuePrefetch;
    }

}  

	
	
	



