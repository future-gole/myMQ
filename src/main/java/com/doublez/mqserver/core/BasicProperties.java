package com.doublez.mqserver.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class BasicProperties implements Serializable {
    //消息的唯一身份标识
    private String messageId;
    //消息上的内容，与bindingkey做匹配
    //DIRECT -> 表示要转发的队列名字
    //FANOUT -> 无意义
    //TOPIC -> rountingKey 与 bindingKey 做匹配
    private String routingKey;
    //表示属性是否要持久化，1 -> 不持久化 2 -> 持久化
    private int deliveryMode = 1;

}
