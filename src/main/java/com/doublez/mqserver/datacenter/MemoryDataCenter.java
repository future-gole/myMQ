package com.doublez.mqserver.datacenter;

import com.doublez.common.MqException;
import com.doublez.mqserver.core.Binding;
import com.doublez.mqserver.core.Exchange;
import com.doublez.mqserver.core.MSGQueue;
import com.doublez.mqserver.core.Message;
import lombok.extern.slf4j.Slf4j;


import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MemoryDataCenter {
    //交互机（Exchange）：使用`HashMap`，`key`: ExchangeName, value: Exchange Object
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();
    //队列（MSGQueue）：使用`HashMap`，`key:` MSGQueueName, `value`: MSGQueue Object
    private ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();
    //绑定（Binding）：嵌套的`HashMap`，`key`: ExchangeName, `value`: HashMap(`key`: queueName,` value`: Binding Object)
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Binding>> bindingsMap = new ConcurrentHashMap<>();
    //消息（Message）：使用`HashMap`，`key`: MessageId, `value`: Message Object
    private ConcurrentHashMap<String,Message> messageMap = new ConcurrentHashMap<>();
    //队列和消息直接的关联：`嵌套的HashMap`, `key`: queueName, `value`: LinkedList(每个元素都是Message Object)
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessageMap = new ConcurrentHashMap<>();
    //未被确认的消息：`嵌套的HashMap`, `key`: queueName, `value`: HashMap(key: messageId, value: Message)
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> queueMessageWaitACK = new ConcurrentHashMap<>();

    public void insertExchange(Exchange exchange) {
        exchangeMap.put(exchange.getName(), exchange);
        log.info("[MemoryDataCenter] 交互机添加成功, exchangeName：{}" ,exchange.getName());
    }

    public Exchange getExchange(String exchangeName) {
        return exchangeMap.get(exchangeName);
    }
    public void deleteExchange(String exchangeName) {
        exchangeMap.remove(exchangeName);
        log.info("[MemoryDataCenter] 交互机删除成功, exchangeName：{}" ,exchangeName);
    }
    public void insertQueue(MSGQueue queue) {
        queueMap.put(queue.getName(), queue);
        log.info("[MemoryDataCenter] 队列添加成功, queueName：{}" ,queue.getName());
    }

    public MSGQueue getQueue(String queueName) {
        return queueMap.get(queueName);
    }

    public void deleteQueue(String queueName) {
        queueMap.remove(queueName);
        log.info("[MemoryDataCenter] 队列删除成功, queueName：{}", queueName);
    }

    public void insertBinding(Binding binding) throws MqException {
        //1. 先查找 exchangeName 对于的hash表是否存在
//        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
//        if(bindingMap == null) {
//            bindingMap = new ConcurrentHashMap<>();
//            bindingsMap.put(binding.getExchangeName(), bindingMap);
//        }
        //上述方法等价于
        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
        //根据queueName查询一下是否存在，存在抛异常，不存在插入
        synchronized(bindingMap) {
            if (bindingMap.containsKey(binding.getQueueName())) {
                throw new MqException("[MemoryDataCenter] 绑定已经存在！ exchange :" + binding.getExchangeName() + ", queueName : " + binding.getQueueName());
            }
            //bindingsMap 键值对中的bindingMap存的是对象的引用，这里直接put就可以，不需要再put到bindingsMap中
            bindingMap.put(binding.getQueueName(), binding);
        }
        log.info("[MemoryDataCenter] 绑定添加成功, queueName: {}, exchangeName:{}",binding.getQueueName(),binding.getExchangeName());
        //下面方法等价于上面，但是异常无法抛出（除非使用一个标志位）
//        bindingsMap.compute(binding.getExchangeName(), (exchangeName, bindingMap) -> {
//            if (bindingMap == null) {
//                bindingMap = new ConcurrentHashMap<>();
//            }
//            if (bindingMap.containsKey(binding.getQueueName())) {
//                throw new MqException("[MemoryDataCenter] 绑定已经存在！ exchange :" + binding.getExchangeName() + ", queueName : " + binding.getQueueName());
//            }
//            bindingMap.put(binding.getQueueName(), binding);
//            return bindingMap;
//        });
    }

    public Binding getBinding(String exchangeName , String queueName) {
//        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.get(exchangeName);
//        if(bindingMap.containsKey(queueName)) {
//            return bindingMap.get(queueName);
//        }
//        return null;
    //上述方法可以用下面的方法来代替
        return Optional.ofNullable(bindingsMap.get(exchangeName))
                .map(bindingMap -> bindingMap.get(queueName))
                .orElse(null);
    }

    public ConcurrentHashMap<String, Binding> getBindings(String exchangeName) {
        return bindingsMap.get(exchangeName);
    }

    public void deleteBinding(Binding binding) throws MqException {
        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
        if(!bindingMap.containsKey(binding.getQueueName())) {
            throw new MqException("[MemoryDataCenter] 绑定不存在 exchange :" + binding.getExchangeName() + ", queueName : " + binding.getQueueName());
        }
        bindingMap.remove(binding.getQueueName());
        log.info("[MemoryDataCenter] 绑定删除成功, queueName: {}, exchangeName:{}",binding.getQueueName(),binding.getExchangeName());
    }

    public void addMessage(Message message) {
        messageMap.put(message.getMessageId(), message);
        log.info("[MemoryDataCenter] 添加信息成功, messageId:{}",message.getMessageId());
    }
    public Message getMessage(String messageId) {
        return messageMap.get(messageId);
    }
    public void removeMessage(String messageId) {
        messageMap.remove(messageId);
        log.info("[MemoryDataCenter] 删除信息成功, messageId:{}",messageId);
    }
    //发送消息
    public void sendMessage(MSGQueue queue, Message message) {
//        LinkedList<Message> messages = queueMessageMap.get(queue.getName());
//        //判断是否为空
//        if(messages == null) {
//            messages = new LinkedList<>();
//        }
//        messages.add(message);
        LinkedList<Message> messages = queueMessageMap.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
        //TODO 这里锁可以进一步优化
        synchronized(messages) {
            messages.add(message);
        }
        //添加一份到消息中心
        addMessage(message);
        log.info("[MemoryDataCenter] 添加信息到队列中成功，queueName:{}, messageId: {}",queue.getName(),message.getMessageId());
    }
    //获取消息
    public Message pollQueueMessages(String queueName) {
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null || messages.isEmpty()) {
            return null;
        }
        Message currentMessage = messages.poll();
        log.info("[MemoryDataCenter] 信息从队列取出成功，queueName:{}, messageId: {}",queueName,currentMessage.getMessageId());
        return currentMessage;
    }
    //获取消息数量
    public int getMessageCount(String queueName) {
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null || messages.isEmpty()) {
            return 0;
        }
        //加锁
        synchronized (messages) {
            return messages.size();
        }
    }
    //添加未确认的消息
    public void addMessageWaitACK(String queueName, Message message) {
         queueMessageWaitACK.computeIfAbsent(queueName, k -> new ConcurrentHashMap<>()).put(message.getMessageId(), message);
        log.info("[MemoryDataCenter] 信息进入待确认队列，queueName:{}, messageId: {}",queueName,message.getMessageId());
    }
    //删除未确认消息
    public void removeMessageWaitACK(String queueName,  String messageId) {
        ConcurrentHashMap<String,Message> messageMap = queueMessageWaitACK.get(queueName);
        if(messageMap == null) {
            return;
        }
        messageMap.remove(messageId);
        log.info("[MemoryDataCenter] 信息从待确认队列删除，queueName:{}, messageId: {}",queueName,messageId);
    }
    //获取未确认的消息
    public Message getMessageWaitACK(String queueName, String messageId) {
        ConcurrentHashMap<String,Message> messageMap = queueMessageWaitACK.get(queueName);
        if(messageMap == null) {
            return null;
        }
        return messageMap.get(messageId);
    }
    // 从硬盘中读取数据，把持久化存储的数据恢复到内存中
    public void recovery(DiskDataCenter diskDataCenter) throws IOException, MqException, ClassNotFoundException {
        //清空所有数据
        exchangeMap.clear();
        queueMap.clear();
        bindingsMap.clear();
        messageMap.clear();
        queueMessageMap.clear();
        queueMessageWaitACK.clear();
        //1. 恢复所有的交互机数据
        for(Exchange exchange : diskDataCenter.selectAllExchange()){
            exchangeMap.put(exchange.getName(), exchange);
        }
        //2. 恢复所有的队列数据 以及 队列和消息之间的关系 以及 消息数据
        for(MSGQueue queue : diskDataCenter.selectAllQueue()){
            LinkedList<Message> messages = diskDataCenter.loadMessageFromQueue(queue.getName());
            //恢复消息数据
            for(Message message : messages){
                messageMap.put(message.getMessageId(), message);
            }
            //恢复所有的队列数据
            queueMap.put(queue.getName(), queue);
            // 队列和消息之间的关系
            queueMessageMap.put(queue.getName(), messages);
        }
        //3. 恢复所有的绑定数据
        for(Binding binding : diskDataCenter.selectAllBinding()){
            ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
            bindingMap.put(binding.getQueueName(), binding);
        }
        //4. 未确认的消息不需要从硬盘中读取，因为等待ack的时候如果服务重启，那么就会被恢复成“未被取走的消息”


    }
}
