package com.doublez.common;

import lombok.Data;

import java.io.Serializable;

/**
 * 公共的返回参数
 */
@Data
public class BasicReturns implements Serializable {

    protected String rid;

    protected String channelId;
    //表示远程调用方法的返回值
    protected boolean ok;
}
