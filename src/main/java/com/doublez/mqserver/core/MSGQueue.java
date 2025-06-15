package com.doublez.mqserver.core;

import com.doublez.common.ConsumerEnv;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 标识队列
 */
@Data
@Slf4j
public class MSGQueue {
    //队列标识名称
    private String name;
    //标识队列是否持久化
    private boolean durable = false;
    //标识队列是否启用 独占功能
    private boolean exclusive = false;
    //标识是否自动删除
    private boolean autoDelete = false;
    //设置创建交换机时传入的参数
    private Map<String,Object> arguments = new HashMap<>();

    //当前队列的消费者
    private List<ConsumerEnv> consumerEnvList = new ArrayList<>();

    private AtomicInteger consumerSeq = new AtomicInteger(0);
    //添加新的订阅者
    public void addConsumerEnv(ConsumerEnv env) {
            consumerEnvList.add(env);
    }
    //todo 删除订阅者
    //获取订阅者
    public ConsumerEnv chooseConsumer() {
        if(consumerEnvList.isEmpty()) {
            //无消费者
            return null;
        }
        int cur = consumerSeq.get() % consumerEnvList.size();
        consumerSeq.getAndIncrement();
        return consumerEnvList.get(cur);
    }
    //mybatis在转化的时候需要get set方法，但是只能接受String，所以要在内部进行类型转化
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

    public void setArguments(String argumentsJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.arguments = mapper.readValue(argumentsJson, new TypeReference<Map<String,Object>>() {});
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
    }

    public void setArguments(Map<String,Object> arguments) {
        this.arguments = arguments;
    }

}
