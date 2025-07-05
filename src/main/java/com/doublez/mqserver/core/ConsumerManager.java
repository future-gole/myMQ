package com.doublez.mqserver.core;

import com.doublez.common.Consumer;
import com.doublez.common.ConsumerEnv;
import com.doublez.common.MqException;
import com.doublez.mqserver.VirtualHost;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 实现消费者的核心操作
 */
@Slf4j
public class ConsumerManager {
    //持有上层的 virtualHost 对象引用，用来操作数据
    private VirtualHost parent;
    //指定线程池，负责执行具体的回调任务
    private ExecutorService workerPool = Executors.newFixedThreadPool(4);
    // 存放令牌队列
    private BlockingQueue<String> tokenQueue = new LinkedBlockingQueue<String>();
    //扫描线程
    private Thread scannerThread = null;
    public ConsumerManager(VirtualHost parent) {
        this.parent = parent;

        scannerThread = new Thread(() -> {
            while (true) {
                //1. 获取队列令牌
                try {
                    String queueName = tokenQueue.take();
                    MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
                    if(queue==null) {
                        throw new MqException("[ConsumerManager] 取令牌后发现队列不存在，queueName:{}" + queueName);
                    }
                    consumeMessage(queue);
                } catch (InterruptedException | MqException e) {
                    log.error("[ConsumerManager] 扫描线程出错，error:{} ", e.getMessage());
                }
            }
        });
        //设置为后台线程
        scannerThread.setDaemon(true);
        //线程持续运行
        scannerThread.start();
    }

    //发送消息的时候进行调用
    public void notifyConsumer(String queueName) throws InterruptedException {
        tokenQueue.put(queueName);
    }

    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) throws MqException {
        //1. 找到对应的队列
        MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
        if(queue == null) {
            throw  new MqException("[ConsumerManager] 队列不存在，queue:" + queueName);
        }
        //2. 添加消费者
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag,queueName,autoAck,consumer);
        synchronized (queue) {
            queue.addConsumerEnv(consumerEnv);
            //2.1 如果当前队列有消息，直接进行消费
            int count = parent.getMemoryDataCenter().getMessageCount(queueName);
            for(int i = 0; i < count; i++) {
                consumeMessage(queue);
            }
        }

    }

    private void consumeMessage(MSGQueue queue) {
        //1. 取消费者
        ConsumerEnv luckyDog = queue.chooseConsumer();
        if(luckyDog == null) {
            //无消费者 不消费
            return;
        }
        //2. 取消息
        Message message = parent.getMemoryDataCenter().pollQueueMessages(queue.getName());
        if(message == null){
            return;
        }
        //3. 消息放入消费者的回调方法中，丢给线程池
        workerPool.submit(() -> {
            try {
                //3.1 将消息添加到 待确认 集合中
                parent.getMemoryDataCenter().addMessageWaitACK(queue.getName(),message);
                //3.2 执行回调函数
                luckyDog.getConsumer().handleDelivery(luckyDog.getConsumerTag(),message.getBasicProperties(),message.getBody());
                //3.3 根据是否为自动应答来进行后续操作
                if(luckyDog.isAutoAck()){
                    if(message.getDeliveryMode() == 2){
                        //3.3.1 删除硬盘消息
                        parent.getDiskDataCenter().deleteMessage(queue,message);
                    }
                    //3.3.2 删除内存消息
                    parent.getMemoryDataCenter().removeMessage(message.getMessageId());
                    //3.3.3 删除未确认消息
                    parent.getMemoryDataCenter().removeMessageWaitACK(queue.getName(),message.getMessageId());
                }
                log.info("[ConsumerManager] 消费消息成功，queueName:{}",queue.getName());
                //不是自动应答的话由消费者调用basicAck来调用
            }catch (IOException | MqException | ClassNotFoundException e) {
                //todo 如果在中间有异常抛出可能会导致后面的消息无法被易错，需要参考rabbitmq的死信队列实现
                log.error("[ConsumerManager] 消费信息出错， error:{} ", e.getMessage());
            }
        });
    }
}
