package com.doublez.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 表示消费者完整的执行环境
 */
@Data
@AllArgsConstructor
public class ConsumerEnv {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
    // 通过回调函数来处理收到的消息
    private Consumer consumer;
}
