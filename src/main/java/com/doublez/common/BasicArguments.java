package com.doublez.common;

import lombok.Data;

import java.io.Serializable;

/**
 * 表示方法的公共参数和辅助字段，作为父类
 */
@Data
public class BasicArguments implements Serializable {
    //表示一个响应/请求的身份标识，把请求和响应对应
    protected String rid;
    //标识本次通信使用的channel
    protected String channelId;
}
