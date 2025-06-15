package com.doublez.common;

import lombok.Data;

/**
 * 表示网络通信的响应对象，按照自定义协议格式展开
 */
@Data
public class Response {
    private int type;
    private int length;
    private byte[] payload;
}
