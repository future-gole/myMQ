package com.doublez.mqserver.datacenter;

import com.doublez.common.MqException;
import com.doublez.mqserver.core.Binding;
import com.doublez.mqserver.core.Exchange;
import com.doublez.mqserver.core.MSGQueue;
import com.doublez.mqserver.core.Message;
import lombok.Data;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 统一管理数据类，对使用者进行封装
 */
public class DiskDataCenter {
    //管理数据库中的数据
    private DataBaseManager dataBaseManager = new DataBaseManager();
    //管理数据文件中的数据
    private MessageFileManager messageFileManager = new MessageFileManager();

    public void init(){
        dataBaseManager.init();
        messageFileManager.init();
    }

    //封装交换机操作
    public void insertExchange(Exchange exchange){
        dataBaseManager.insertExchange(exchange);
    }

    public void deleteExchange(String exchangeName){
        dataBaseManager.deleteExchange(exchangeName);
    }
    public List<Exchange> selectAllExchange(){
        return dataBaseManager.selectAllExchange();
    }
    //封装绑定操作
    public void insertBinding(Binding binding){
        dataBaseManager.insertBinding(binding);
    }
    public void deleteBinding(Binding binding){
        dataBaseManager.deleteBinding(binding);
    }
    public List<Binding> selectAllBinding(){
        return dataBaseManager.selectAllBinding();
    }
    //封装队列操作
    public void insertQueue(MSGQueue queue) throws IOException, MqException {
        dataBaseManager.insertQueue(queue);
        //需要创建队列文件，消息存储的时候需要检验
        messageFileManager.createQueueFiles(queue.getName());

    }
    public void deleteQueue(String queueName) throws IOException {
        dataBaseManager.deleteQueue(queueName);
        //需要删除队列文件，消息存储的时候需要检验
        messageFileManager.destroyQueueFiles(queueName);
    }
    public List<MSGQueue> selectAllQueue(){
        return dataBaseManager.selectAllQueue();
    }

    //封装消息操作
    public void sendMessage(MSGQueue queue, Message message) throws IOException, MqException {
        messageFileManager.sendMessage(queue,message);
    }
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, MqException, ClassNotFoundException {
        messageFileManager.deleteMessage(queue,message);
        if(messageFileManager.checkGC(queue.getName())){
            messageFileManager.gc(queue);
        }
    }
    public LinkedList<Message> loadMessageFromQueue(String queueName) throws MqException, ClassNotFoundException, IOException {
        return messageFileManager.loadMessageFromQueue(queueName);
    }
}
