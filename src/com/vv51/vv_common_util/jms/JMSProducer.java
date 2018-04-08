package com.vv51.vv_common_util.jms;

import java.util.Map;
import java.util.Set;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vv51.vv_common_util.other.ExceptionDump;

public class JMSProducer implements ExceptionListener{
    private static Logger logger = LogManager.getLogger(JMSProducer.class);
    
    public final static int     JMS_DEFAULT_MAX_CONNECTIONS = 32;
    public final static int     JMS_DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION = 300;
    public final static boolean JMS_DEFAULT_USE_ASYNC_SEND_FOR_JMS = false;
    public final static boolean JMS_DEFAULT_IS_PERSISTENT = false;    
    
    private int mMaxConnections = JMS_DEFAULT_MAX_CONNECTIONS;
    
    //设置每个连接中使用的最大活动会话数
    private int mMaximumActiveSessionPerConnection = JMS_DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION;

    //强制使用同步返回数据的格式
    private boolean mUseAsyncSendForJMS = JMS_DEFAULT_USE_ASYNC_SEND_FOR_JMS;

    //是否持久化消息
    private boolean mIsPersistent = JMS_DEFAULT_IS_PERSISTENT;
 
    //连接地址
    private String mBrokerUrl = "";
    private String mUserName  = "";
    private String mPassword  = "";

    private PooledConnectionFactory mConnectionFactory = null;

    public JMSProducer(String brokerUrl, String userName, String password) {
        this(brokerUrl, userName, password, 
                JMS_DEFAULT_MAX_CONNECTIONS, 
                JMS_DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION,
                JMS_DEFAULT_USE_ASYNC_SEND_FOR_JMS, 
                JMS_DEFAULT_IS_PERSISTENT);
    }
    
    /**  
     * 构造函数 
     *  
     * @param brokerUrl
     * @param userName
     * @param password
     * @param maxConnections
     * @param maximumActiveSessionPerConnection
     * @param useAsyncSendForJMS
     * @param isPersistent
     */   
    public JMSProducer(String brokerUrl, String userName, String password, int maxConnections, int maximumActiveSessionPerConnection, boolean useAsyncSendForJMS, boolean isPersistent) {
        this.mUseAsyncSendForJMS = useAsyncSendForJMS;
        this.mIsPersistent = isPersistent;
        this.mBrokerUrl = brokerUrl;
        this.mUserName = userName;
        this.mPassword = password;
        this.mMaxConnections = maxConnections;
        this.mMaximumActiveSessionPerConnection = maximumActiveSessionPerConnection;
        init();
    }
      
    private void init() {  
        //ActiveMQ的连接工厂
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(this.mUserName, this.mPassword, this.mBrokerUrl);
        activeMQConnectionFactory.setUseAsyncSend(this.mUseAsyncSendForJMS);
        activeMQConnectionFactory.setProducerWindowSize(1024000);
        
        //Active中的连接池工厂
        this.mConnectionFactory = new PooledConnectionFactory(activeMQConnectionFactory);
        this.mConnectionFactory.setCreateConnectionOnStartup(true);
        this.mConnectionFactory.setMaxConnections(this.mMaxConnections);
        this.mConnectionFactory.setMaximumActiveSessionPerConnection(this.mMaximumActiveSessionPerConnection);
    }
    
    /**
     * 执行消息发送
     * @param queue
     * @param map
     * @throws Exception
     */
    public void sendQueueMsg(String queue, Map<String, Object> map, long delay) throws Exception {
        Connection connection = null;
        Session session = null;
        try {
            //从连接池工厂中获取一个连接
            connection = this.mConnectionFactory.createConnection();
            /*createSession(boolean transacted,int acknowledgeMode)
              transacted - indicates whether the session is transacted acknowledgeMode - indicates whether the consumer or the client 
              will acknowledge any messages it receives; ignored if the session is transacted. 
              Legal values are Session.AUTO_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE, and Session.DUPS_OK_ACKNOWLEDGE.
            */
            //false 参数表示 为非事务型消息，后面的参数表示消息的确认类型
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            //Destination is superinterface of Queue
            //PTP消息方式     
            Destination destination = session.createQueue(queue);
            //Creates a MessageProducer to send messages to the specified destination
            MessageProducer producer = session.createProducer(destination);
            //set delevery mode
            producer.setDeliveryMode(this.mIsPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            //map convert to javax message
            Message message = getMapMessage(session, map);
            if (0 < delay) {
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            }
            producer.send(message);
        } catch (Exception e) {
            logger.error("JMSProducer sendQueueMsg error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            throw e;
        } finally {
            closeSession(session);
            closeConnection(connection);
        }
    }

    public void sendQueueMsg(String queue, String text) throws Exception {
        sendQueueMsg(queue, text, 0, null);
    }

    /**
     * 执行消息发送
     * @param queue
     * @param text
     * @param delay
     * @throws Exception
     */
    public void sendQueueMsg(String queue, String text, long delay, String type) throws Exception {
        Connection connection = null;
        Session session = null;
        try {
            //从连接池工厂中获取一个连接
            connection = this.mConnectionFactory.createConnection();
            /*createSession(boolean transacted,int acknowledgeMode)
              transacted - indicates whether the session is transacted acknowledgeMode - indicates whether the consumer or the client 
              will acknowledge any messages it receives; ignored if the session is transacted. 
              Legal values are Session.AUTO_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE, and Session.DUPS_OK_ACKNOWLEDGE.
            */
            //false 参数表示 为非事务型消息，后面的参数表示消息的确认类型
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            //Destination is superinterface of Queue
            //PTP消息方式     
            Destination destination = session.createQueue(queue);
            //Creates a MessageProducer to send messages to the specified destination
            MessageProducer producer = session.createProducer(destination);
            //set delevery mode
            producer.setDeliveryMode(this.mIsPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            //map convert to javax message
            Message message = getTextMessage(session, text);
            if (0 < delay) {
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            }
            if (null != type) {
                message.setJMSType(type);
            }
            producer.send(message);
        } catch (Exception e) {
            logger.error("JMSProducer sendQueueMsg error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            throw e;
        } finally {
            closeSession(session);
            closeConnection(connection);
        }
    }

    public void sendQueueMsg(String queue, byte[] text) throws Exception {
        sendQueueMsg(queue, text, 0, null);
    }

    /**
     * 执行消息发送
     * @param queue
     * @param text
     * @param delay
     * @throws Exception
     */
    public void sendQueueMsg(String queue, byte[] text, long delay, String type) throws Exception {
        Connection connection = null;
        Session session = null;
        try {
            //从连接池工厂中获取一个连接
            connection = this.mConnectionFactory.createConnection();
            /*createSession(boolean transacted,int acknowledgeMode)
              transacted - indicates whether the session is transacted acknowledgeMode - indicates whether the consumer or the client
              will acknowledge any messages it receives; ignored if the session is transacted.
              Legal values are Session.AUTO_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE, and Session.DUPS_OK_ACKNOWLEDGE.
            */
            //false 参数表示 为非事务型消息，后面的参数表示消息的确认类型
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            //Destination is superinterface of Queue
            //PTP消息方式
            Destination destination = session.createQueue(queue);
            //Creates a MessageProducer to send messages to the specified destination
            MessageProducer producer = session.createProducer(destination);
            //set delevery mode
            producer.setDeliveryMode(this.mIsPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            //map convert to javax message
            Message message = getByteMessage(session, text);
            if (0 < delay) {
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            }
            if (null != type) {
                message.setJMSType(type);
            }
            producer.send(message);
        } catch (Exception e) {
            logger.error("JMSProducer sendQueueMsg error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            throw e;
        } finally {
            closeSession(session);
            closeConnection(connection);
        }
    }
    
    /**
     * 执行消息发送
     * @param topic
     * @param map
     * @throws Exception
     */
    public void sendTopicMsg(String topic, Map<String, Object> map, long delay) throws Exception {
        Connection connection = null;
        Session session = null;
        try {
            //从连接池工厂中获取一个连接
            connection = this.mConnectionFactory.createConnection();
            /*createSession(boolean transacted,int acknowledgeMode)
              transacted - indicates whether the session is transacted acknowledgeMode - indicates whether the consumer or the client 
              will acknowledge any messages it receives; ignored if the session is transacted. 
              Legal values are Session.AUTO_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE, and Session.DUPS_OK_ACKNOWLEDGE.
            */
            //false 参数表示 为非事务型消息，后面的参数表示消息的确认类型
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            //Destination is superinterface of Queue
            //PTP消息方式     
            //Destination destination = session.createQueue(queue);
            Destination destination = session.createTopic(topic);
            //Creates a MessageProducer to send messages to the specified destination
            MessageProducer producer = session.createProducer(destination);
            //set delevery mode
            producer.setDeliveryMode(this.mIsPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            //map convert to javax message
            Message message = getMapMessage(session, map);
            if (0 < delay) {
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            }
            producer.send(message);
        } catch (Exception e) {
            logger.error("JMSProducer sendTopicMsg error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            throw e;
        } finally {
            closeSession(session);
            closeConnection(connection);
        }
    }
    
    /**
     * 执行消息发送
     * @param topic
     * @param text
     * @param delay
     * @throws Exception
     */
    public void sendTopicMsg(String topic, String text, long delay) throws Exception {
        Connection connection = null;
        Session session = null;
        try {
            //从连接池工厂中获取一个连接
            connection = this.mConnectionFactory.createConnection();
            /*createSession(boolean transacted,int acknowledgeMode)
              transacted - indicates whether the session is transacted acknowledgeMode - indicates whether the consumer or the client 
              will acknowledge any messages it receives; ignored if the session is transacted. 
              Legal values are Session.AUTO_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE, and Session.DUPS_OK_ACKNOWLEDGE.
            */
            //false 参数表示 为非事务型消息，后面的参数表示消息的确认类型
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            //Destination is superinterface of Queue
            //PTP消息方式     
            //Destination destination = session.createQueue(queue);
            Destination destination = session.createTopic(topic);
            //Creates a MessageProducer to send messages to the specified destination
            MessageProducer producer = session.createProducer(destination);
            //set delevery mode
            producer.setDeliveryMode(this.mIsPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            //map convert to javax message
            Message message = getTextMessage(session, text);
            if (0 < delay) {
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            }
            producer.send(message);
        } catch (Exception e) {
            logger.error("JMSProducer sendTopicMsg error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            throw e;
        } finally {
            closeSession(session);
            closeConnection(connection);
        }
    }

    /**
     * 执行消息发送
     * @param topic
     * @param text
     * @param delay
     * @throws Exception
     */
    public void sendTopicMsg(String topic, byte[] text, long delay) throws Exception {
        Connection connection = null;
        Session session = null;
        try {
            //从连接池工厂中获取一个连接
            connection = this.mConnectionFactory.createConnection();
            /*createSession(boolean transacted,int acknowledgeMode)
              transacted - indicates whether the session is transacted acknowledgeMode - indicates whether the consumer or the client
              will acknowledge any messages it receives; ignored if the session is transacted.
              Legal values are Session.AUTO_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE, and Session.DUPS_OK_ACKNOWLEDGE.
            */
            //false 参数表示 为非事务型消息，后面的参数表示消息的确认类型
            session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
            //Destination is superinterface of Queue
            //PTP消息方式
            //Destination destination = session.createQueue(queue);
            Destination destination = session.createTopic(topic);
            //Creates a MessageProducer to send messages to the specified destination
            MessageProducer producer = session.createProducer(destination);
            //set delevery mode
            producer.setDeliveryMode(this.mIsPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            //map convert to javax message
            Message message = getByteMessage(session, text);
            if (0 < delay) {
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            }
            producer.send(message);
        } catch (Exception e) {
            logger.error("JMSProducer sendTopicMsg error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            throw e;
        } finally {
            closeSession(session);
            closeConnection(connection);
        }
    }

    private Message getMapMessage(Session session, Map<String, Object> map) throws JMSException {
        MapMessage message = session.createMapMessage();
        if (null != map && !map.isEmpty()) {
            Set<String> keys = map.keySet();
            for (String key : keys) {
                message.setObject(key, map.get(key));
            }
        }
        return message;
    }
    
    private Message getByteMessage(Session session, byte[] text) throws JMSException {
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(text);
        return message;
    }

    private Message getTextMessage(Session session, String text) throws JMSException {
        TextMessage message = session.createTextMessage();
        message.setText(text);
        return message;
    }
    
    private void closeSession(Session session) {
        try {
            if (null != session) {
                session.close();
            }
        } catch (Exception e) {
            logger.error("JMSProducer closeSession error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }

    private void closeConnection(Connection connection) {
        try {
            if (null != connection) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("JMSProducer closeConnection error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }
    
    @Override
    public void onException(JMSException e) {
        logger.error("JMSProducer onException! " + ExceptionDump.getErrorInfoFromException(e));
        e.printStackTrace();
    }
}


