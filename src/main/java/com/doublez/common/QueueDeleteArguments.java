package com.doublez.common;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper=false)
public class QueueDeleteArguments extends BasicArguments implements Serializable{
    private String queueName;
}
