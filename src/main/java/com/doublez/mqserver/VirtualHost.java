package com.doublez.mqserver;

import com.doublez.common.Consumer;
import com.doublez.common.MqException;
import com.doublez.mqserver.core.*;
import com.doublez.mqserver.datacenter.DiskDataCenter;
import com.doublez.mqserver.datacenter.MemoryDataCenter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Data
public class VirtualHost {
    private String virtualHostName;
    private Router router = new Router();
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();

    private final Object exchangeLock = new Object();
    private final Object queueLock = new Object();
    private ConsumerManager consumerManager = new ConsumerManager(this);

    public VirtualHost(String virtualHostName) {
        this.virtualHostName = virtualHostName;

        //memoryDataCenter不需要进行初始化操作，new对象就可以了
        //diskDataCenter 需要进行初始化操作，进行对应的建库和初始化数据
        diskDataCenter.init();

        //如果硬盘有数据，需要恢复到内存中
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | MqException | ClassNotFoundException e) {
            log.error(e.getMessage());
            log.error("[VirtualHost] 恢复数据失败]");
        }
    }
    //添加交换机
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable,
                                       boolean autoDelete , Map<String,Object> arguments){
        //1. 交换机的名字添加虚拟主机的名字作为前缀
        exchangeName = virtualHostName + exchangeName;

        //2. 创建交换机
        //2.1 先判断交互机是否已经存在
        try {
            //加锁防止多线程创建相同名字的交换机
            synchronized (exchangeLock) {
                Exchange existExchange = memoryDataCenter.getExchange(exchangeName);
                if (existExchange != null) {
                    log.info("[VirtualHost] 交互机已经存在，exchangeName:{}", exchangeName);
                    return true;
                }
                Exchange exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);
                //如果持久化那么存入磁盘（先写硬盘防止硬盘写入失败，不会污染内存数据）
                if (durable) {
                    diskDataCenter.insertExchange(exchange);
                }
                //存入内存
                memoryDataCenter.insertExchange(exchange);
                log.info("[VirtualHost] 交互机插入，exchangeName: {}", exchangeName);
            }
            return true;
        } catch (Exception e) {
            log.error("[VirtualHost] 插入交互机失败: {}",e.getMessage());
        }
        return false;
    }
    //删除交换机
    public boolean exchangeDelete(String exchangeName){
        exchangeName = virtualHostName + exchangeName;
        //1. 判断交换机是否存在
        try {
            //加锁防止删除相同名字的交换机
            synchronized (exchangeLock) {
                Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                if (exchange == null) {
                    log.error("[VirtualHost] 交换机不存在无法删除：exchangeName:{}", exchangeName);
                    return false;
                }

                //2. 删除交换机
                memoryDataCenter.deleteExchange(exchangeName);
                if (exchange.isDurable()) {
                    diskDataCenter.deleteExchange(exchangeName);
                }
                log.error("[VirtualHost] 删除交换机成功：exchangeName:{}", exchangeName);
            }
            return true;
        } catch (Exception e) {
            log.error("[VirtualHost] 删除交互机失败: {}",e.getMessage());
            return false;
        }
    }
    //创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive,
                                boolean autoDelete , Map<String,Object> arguments){
        queueName = virtualHostName + queueName;
        try {
            //加锁防止多线程创建相同名字的队列
            synchronized (queueLock) {
                if(memoryDataCenter.getQueue(queueName) != null){
                    log.error("[VirtualHost] 队列已经存在无法插入，queueName: {}",queueName);
                }
                MSGQueue queue = new MSGQueue();
                queue.setName(queueName);
                queue.setExclusive(exclusive);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);
                queue.setDurable(durable);

                if(durable){
                    diskDataCenter.insertQueue(queue);
                }
                memoryDataCenter.insertQueue(queue);
                log.info("[VirtualHost] 插入新的队列成功，queueName: {}",queueName);
            }
            return true;
        } catch (IOException | MqException e) {
            log.error("[VirtualHost] 插入新的队列失败，queueName: {}",queueName);
            return false;
        }
    }
    //删除队列
    public boolean queueDelete(String queueName){
        queueName = virtualHostName + queueName;

        try {
            //加锁防止多线程删除相同名字的队列
            synchronized (queueLock) {
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if( queue == null){
                    log.error("[VirtualHost] 队列不存在无法删除：queueName:{}",queueName);
                    return false;
                }
                memoryDataCenter.deleteQueue(queueName);
                if(queue.isDurable()){
                    diskDataCenter.deleteQueue(queueName);
                }
            }
            return true;
        } catch (IOException e) {
            log.error("[VirtualHost] 删除队列失败: {}",e.getMessage());
            return false;
        }
    }

    //创建绑定
    public boolean queueBind(String queueName, String exchangeName, String bindingKey){
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;
        //1. 判断绑定是否存在
        try{
            synchronized (exchangeLock) {
                synchronized (queueLock) {
                    Binding binding = memoryDataCenter.getBinding(exchangeName,queueName);
                    if(binding != null){
                        log.error("[VirtualHost] 绑定关系已经存在无法添加, exchangeName: {} , queueName: {}",exchangeName,queueName);
                        return false;
                    }
                    //2. 判断绑定是否合法
                    if(!router.checkBindingKey(bindingKey)){
                        log.error("[VirtualHost] 绑定关系不合法, routingKey: {}",bindingKey);
                        return false;
                    }
                    //3. 判断队列和交换机是否存在
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    if(queue == null){
                        log.error("[VirtualHost] 队列不存在，无法新增绑定关系，queueName:{}",queueName);
                        return false;
                    }
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    if(exchange == null){
                        log.error("[VirtualHost] 交换机不存在，无法新增绑定关系，exchangeName:{}",exchangeName);
                    }
                    binding = new Binding();
                    binding.setExchangeName(exchangeName);
                    binding.setQueueName(queueName);
                    binding.setBindingKey(bindingKey);

                    //4. 先写入硬盘
                    if(queue.isDurable() && exchange.isDurable()){
                        diskDataCenter.insertBinding(binding);
                    }
                    //5. 写入内存
                    memoryDataCenter.insertBinding(binding);
                    log.info("[VirtualHost] 新增绑定关系成功，exchangeName:{}, queue Name:{}, bindingKey:{}",exchangeName,queueName,bindingKey);
                }
            }
            return true;
        }catch (MqException e){
            log.error("[VirtualHost] 新增绑定关系失败: {}",e.getMessage());
            return false;
        }
    }

    public boolean queueUnbind(String queueName,String exchangeName){
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;
        //1. 查询绑定是否存在
        try {
            //要确保加锁顺序一致
            synchronized (exchangeLock) {
                synchronized (queueLock) {
                    Binding binding = memoryDataCenter.getBinding(exchangeName, queueName);
                    if(binding == null){
                        log.error("[VirtualHost] 绑定不存在无法删除: exchangeName: {} , queueName: {}",exchangeName,queueName);
                        return false;
                    }
        //            MSGQueue queue = memoryDataCenter.getQueue(queueName);
        //            if(queue == null){
        //                log.error("[VirtualHost] 队列不存在，无法删除绑定关系，queueName:{}",queueName);
        //                return false;
        //            }
        //            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
        //            if(exchange == null){
        //                log.error("[VirtualHost] 交换机不存在，无法删除绑定关系，exchangeName:{}",exchangeName);
        //            }
        //            //2. 删除硬盘
        //            if(queue.isDurable() && exchange.isDurable()){
        //                diskDataCenter.deleteBinding(binding);
        //            }
                    //3.删除内存
                    memoryDataCenter.deleteBinding(binding);
                    log.info("[VirtualHost] 删除绑定关系成功，exchangeName:{}, queue Name:{}",exchangeName,queueName);
                }
            }
            return true;
        } catch (Exception e) {
            log.error("[VirtualHost] 删除绑定关系失败: {}",e.getMessage());
            return false;
        }
    }
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body){
        try{
            //1. 转化交换机的名字
            exchangeName = virtualHostName + exchangeName;
            //2. 判断routingKey是否合法
            if(!router.checkRoutingKey(routingKey)){
                log.error("[VirtualHost] routingKey不合法，routingKey: {}",routingKey);
                return false;
            }
            //3. 查找交换机是否存在
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if(exchange == null){
                log.error("[VirtualHost] 交换机不存在, 无法发送消息，exchangeName: {} , routingKey: {}",exchangeName,routingKey);
                return false;
            }
            //4. 判断交换机的类型
            if(exchange.getType() == ExchangeType.DIRECT){
                //4.1 直接交换机
                //4.1.1 查找对应的队列是否存在
                String queueName = virtualHostName + routingKey;
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if(queue == null){
                    log.error("[VirtualHost] queue不存在,无法发送消息，queueName: {}",queueName);
                    return false;
                }
                //4.1.2 创建消息
                Message message = Message.createMessageId(routingKey,basicProperties,body);
                //4.1.3 写入队列
                sendMessage(queue,message);
            }else {
                //4.2 FANOUT和TOPIC交换机
                //4.2.1 找到所有关联的绑定，遍历所有绑定对象
                ConcurrentHashMap<String, Binding> bindings = memoryDataCenter.getBindings(exchangeName);
                for(Map.Entry<String, Binding> entry : bindings.entrySet()){
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(binding.getQueueName());
                    if(queue == null){
                        log.error("[VirtualHost] basicPublish 发送消息时，发现队列不存在！ queueName:{}",binding.getQueueName());
                        continue;
                    }
                    //4.2.2 构造绑定对象
                    Message message = Message.createMessageId(routingKey,basicProperties,body);
                    //判断消息是否能够转发给队列：fanout 所有的队列都要转发，topic 只有bindingKey和routingKey匹配才能转发
                    if(!router.route(exchange.getType(),binding,message)){
                        continue;
                    }
                    //4.2.3 发送消息给队列
                    sendMessage(queue,message);
                    log.info("[VirtualHost] basicPublish 成功,queueName:{}",binding.getQueueName());
                }
            }
            return true;
        }catch (Exception e){
            log.error("[VirtualHost] 消息发送失败: {}",e.getMessage());
            return false;
        }
    }

    private void sendMessage(MSGQueue queue, Message message) throws IOException, MqException, InterruptedException {
        //将消息写入内存/硬盘
        int deliveryMode = message.getDeliveryMode();
        //1. 写入磁盘：1 不持久化， 2 持久化
        if(deliveryMode == 2){
            diskDataCenter.sendMessage(queue,message);
        }
        //2. 写入内存
        memoryDataCenter.sendMessage(queue,message);
        //3. 通知消费者可以消费了
        consumerManager.notifyConsumer(queue.getName());
    }
    //订阅消息
    public boolean basicConsume(String consumerTag, String queueName, boolean autoAck, Consumer consumer){
        // 构造ConsumerEnv对象，找到对应的队列，吧Consumer对象添加到队列种
        queueName = virtualHostName + queueName;
        try{
            consumerManager.addConsumer(consumerTag,queueName,autoAck,consumer);
            log.info("[VirtualHost] basicConsume 成功,queueName:{}",queueName);
            return true;
        }catch (Exception e){
            log.error("[VirtualHost] basicConsume 失败，e:{}",e.getMessage());
            return false;
        }
    }
    //确认消息
    public boolean basicAck(String queueName,String messageId){
        queueName = virtualHostName + queueName;
        try {
            MSGQueue queue = memoryDataCenter.getQueue(queueName);
            if(queue == null){
                log.error("[VirtualHost] ACK的队列不存在，queueName:{}",queueName);
                return false;
            }
            Message message = memoryDataCenter.getMessage(messageId);
            if(message == null){
                log.error("[VirtualHost] ACK的消息不存在，messageId:{}",messageId);
                return false;
            }
            if(message.getDeliveryMode() == 2){
                // 删除硬盘消息
                diskDataCenter.deleteMessage(queue,message);
            }
            //删除内存消息
            memoryDataCenter.removeMessage(messageId);
            //删除待确认集合中的消息
            memoryDataCenter.removeMessageWaitACK(queueName,messageId);
            log.info("[VirtualHost] 确认消息成功，queueName:{},messageId:{}",queueName,messageId);
            return true;
        } catch (Exception e) {
            log.error("[VirtualHost] 确认消息失败，error:{}",e.getMessage());
            return false;
        }
    }
}
