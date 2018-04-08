package com.vv51.vv_common_util.jms;

import javax.jms.Message;


/**
 * 提供消息操作的回调接口
 *
 */
public interface MessageHandler {


    /**
     * 消息回调提供的调用方法
     * @param message
     */
    public void handle(Message message);
}  
