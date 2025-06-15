package com.doublez.common;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper=false)
public class BasicConsumeArguments extends BasicArguments implements Serializable {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
    //不需要传输Consumer,由客户端自行定义实现
}
