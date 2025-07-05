package com.doublez.mqserver.core;

import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

/**
 * 表示要传递的一个消息
 * Message 需要能在网络上进行传输，所以需要能够进行序列号和反序列化
 * 标准库的序列话和反序列号为 Serializable
 */
@Data
public class Message implements Serializable {
    private BasicProperties basicProperties = new BasicProperties();
    private byte[] body;

    //辅助属性,如果 message 需要存储到文件中 [offsetBeg,offsetEnd)
    //不需要被序列化，加上transient 关键字
    private transient long offsetBeg = 0;//消息数据的开头距离文件开头的偏移量（字节）闭
    private transient long offsetEnd = 0;//消息数据的结尾距离文件开头的偏移量（字节）开

    //表示该消息在文件中是否有效，0x1 有效 0x0 无效
    private byte isValid = 0x1;

    //创建一个方法工厂，让方法工厂封装创建message的过程
    public static Message createMessageId(String routingKey,BasicProperties basicProperties,byte[] body) {
        Message message = new Message();
        if(basicProperties != null){
            message.setBasicProperties(basicProperties);
        }
        //UUID生成id，以M-开头作为区分
        message.setMessageId("M-" + UUID.randomUUID());

        message.setRoutingKey(routingKey);

        message.setBody(body);
        return message;
    }
    public String getMessageId() {
        return basicProperties.getMessageId();
    }
    public void setMessageId(String messageId) {
        basicProperties.setMessageId(messageId);
    }

    public String getRoutingKey() {
        return basicProperties.getRoutingKey();
    }
    public void setRoutingKey(String routingKey) {
        basicProperties.setRoutingKey(routingKey);
    }
    public int getDeliveryMode(){
        return basicProperties.getDeliveryMode();
    }
    public void setDeliveryMode(int mode){
        basicProperties.setDeliveryMode(mode);
    }
}
