package com.doublez.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * 标识一个交换机
 */
@Data
@Slf4j
public class Exchange {
    //交互机名字
    private String name;
    //交换机类型
    private ExchangeType type = ExchangeType.DIRECT;
    //是否启动交互机持久化存储
    private boolean durable = false;
    //是否启动自动删除
    private boolean autoDelete = false;
    //设置创建交换机时传入的参数
    private Map<String,Object> arguments = new HashMap<>();

    //mybatis在操作数据库转化的时候需要get set方法，但是只能接受String，所以要在内部进行类型转化
    public String getArguments() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
        //如果抛出异常返回空的字符串
        return "{}";
    }
    //同上
    public void setArguments(String argumentsJson) {
       ObjectMapper mapper = new ObjectMapper();
        try {
            this.arguments = mapper.readValue(argumentsJson, new TypeReference<Map<String,Object>>() {});
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
    }
    //前面的setArguments将Slf4j里面的方法覆盖了，需要显示的写出来
    public void setArguments(Map<String,Object> arguments) {
        this.arguments = arguments;
    }
}
