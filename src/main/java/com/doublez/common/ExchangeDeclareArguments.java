package com.doublez.common;

import com.doublez.mqserver.core.ExchangeType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.util.Map;
@Data
@EqualsAndHashCode(callSuper=false)
public class ExchangeDeclareArguments extends BasicArguments implements Serializable {
    private String exchangeName;
    private ExchangeType exchangeType;
    private boolean durable;
    private boolean autoDelete;
    private Map<String,Object> arguments;

}
