package com.doublez.common;

import com.doublez.mqserver.core.BasicProperties;

import java.io.IOException;

/**
 * 函数式接口，用于收到消息进行回调
 */
@FunctionalInterface
public interface Consumer {
    //方法预期在每次服务器收到消息之后来进行调用，即通过这个方法吧消息推送给对应的消费者
     void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException;
}
