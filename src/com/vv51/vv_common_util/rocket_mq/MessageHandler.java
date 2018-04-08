package com.vv51.vv_common_util.rocket_mq;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created by Kim on 2017/9/29.
 */
public interface MessageHandler {

    /**
     * 消息回调提供的调用方法
     * @param message
     */
    public void handle(MessageExt message);
}
