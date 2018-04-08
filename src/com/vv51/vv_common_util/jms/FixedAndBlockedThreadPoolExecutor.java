package com.vv51.vv_common_util.jms;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vv51.vv_common_util.other.ExceptionDump;

/**
 * 支持阻塞的固定大小的线程池
 *
 */
public class FixedAndBlockedThreadPoolExecutor extends ThreadPoolExecutor {
    private static Logger logger = LogManager.getLogger(FixedAndBlockedThreadPoolExecutor.class);

    //一个可重入的互斥锁 Lock，它具有与使用 synchronized 方法和语句所访问的隐式监视器锁相同的一些基本行为和语义，但功能更强大。
    //使用 lock 块来调用 try，在之前/之后的构造中
    private ReentrantLock mReentrantLock = new ReentrantLock();

    private Condition mCondition = this.mReentrantLock.newCondition();

    public FixedAndBlockedThreadPoolExecutor(int size) {
        super(size, size, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    /**
     * 当线程池中没有空闲线程时,会挂起此方法的调用线程.直到线程池中有线程有空闲线程.
     */
    @Override
    public void execute(Runnable command) {
        //进行同步锁定
        this.mReentrantLock.lock();
        super.execute(command);
        try {
            //如果线程池的数量已经达到最大线程池的数量,则进行挂起操作
            if (getPoolSize() == getMaximumPoolSize()) {
                this.mCondition.await();
            }
        } catch (InterruptedException e) {
            logger.error("FixedAndBlockedThreadPoolExecutor execute error[InterruptedException]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } finally {
            this.mReentrantLock.unlock();
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        try {
            this.mReentrantLock.lock();
            this.mCondition.signal();
        } finally {
            this.mReentrantLock.unlock();
        }
    }


}  
