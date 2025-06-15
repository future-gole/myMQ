package com.doublez.common;

import com.doublez.mqserver.core.BasicProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper=false)
public class SubScribeReturns extends BasicReturns implements Serializable {
    private String consumerTag;
    private BasicProperties basicProperties;
    private byte[] body;
}
