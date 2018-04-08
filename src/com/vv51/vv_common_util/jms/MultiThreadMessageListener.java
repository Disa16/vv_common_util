package com.vv51.vv_common_util.jms;

import java.util.concurrent.ExecutorService;

import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vv51.vv_common_util.other.ExceptionDump;


/**
 * 消息消费者中使用的多线程消息监听服务
 *
 */
public class MultiThreadMessageListener implements MessageListener {
    private static Logger logger = LogManager.getLogger(MultiThreadMessageListener.class);

    //默认线程池数量
    public final static int DEFAULT_HANDLE_THREAD_POOL = 10;
    //最大的处理线程数.
    private int mMaxHandleThreads;
    //提供消息回调调用接口
    private MessageHandler mMessageHandler;

    // 线程池定义
    private ExecutorService mExecutorService;

    public MultiThreadMessageListener(MessageHandler messageHandler){
        this(DEFAULT_HANDLE_THREAD_POOL, messageHandler);
    }

    public MultiThreadMessageListener(int maxHandleThreads, MessageHandler messageHandler){
        this.mMaxHandleThreads = maxHandleThreads;
        this.mMessageHandler = messageHandler;
        //支持阻塞的固定大小的线程池(自行手动创建的)
        this.mExecutorService = new FixedAndBlockedThreadPoolExecutor(this.mMaxHandleThreads);
    }

    /**
     * 监听程序中自动调用的方法
     */
    @Override
    public void onMessage(final Message message) {
        //使用支持阻塞的固定大小的线程池来执行操作
        this.mExecutorService.execute(new Runnable() {
            public void run() {
                try {
                    MultiThreadMessageListener.this.mMessageHandler.handle(message);
                } catch (Exception e) {
                    logger.error("MultiThreadMessageListener onMessage error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
                    e.printStackTrace();
                }
            }
        });
    }

}  