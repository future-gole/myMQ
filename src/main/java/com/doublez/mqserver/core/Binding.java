package com.doublez.mqserver.core;

import lombok.Data;

@Data
public class Binding {
    private String exchangeName;
    private String queueName;
    private String bindingKey;
}
