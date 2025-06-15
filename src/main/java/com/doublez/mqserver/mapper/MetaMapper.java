package com.doublez.mqserver.mapper;

import com.doublez.mqserver.core.Binding;
import com.doublez.mqserver.core.Exchange;
import com.doublez.mqserver.core.MSGQueue;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface MetaMapper {
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();

    void insertExchange(Exchange exchange);
    void insertQueue(MSGQueue queue);
    void insertBinding(Binding binding);
    void deleteExchange(String exchangeName);
    void deleteQueue(String queueName);
    void deleteBinding(Binding binding);

    List<Exchange> selectExchanges();
    List<MSGQueue> selectQueues();
    List<Binding> selectBindings();
}
