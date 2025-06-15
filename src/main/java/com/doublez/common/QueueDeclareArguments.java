package com.doublez.common;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper=false)
public class QueueDeclareArguments extends BasicArguments implements Serializable {
    private String queueName;
    private boolean durable;
    private boolean exclusive;
    private boolean autoDelete;
    private Map<String,Object> arguments;
}
