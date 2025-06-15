package com.doublez.mqserver.datacenter;

import com.doublez.Application;
import com.doublez.common.MqException;
import com.doublez.mqserver.core.*;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootTest
class MemoryDataCenterTest {
    private MemoryDataCenter dataCenter = null;
    @BeforeEach
    public void setUp(){
        dataCenter = new MemoryDataCenter();
    }

    @AfterEach
    public void tearDown(){
        dataCenter = null;
    }

    private Exchange createTestExchange(String exchangeName){
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setAutoDelete(false);
        exchange.setDurable(true);
        exchange.setType(ExchangeType.DIRECT);
        return exchange;
    }

    private MSGQueue createTestQueue(String queueName){
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setAutoDelete(false);
        queue.setDurable(true);
        queue.setExclusive(false);
        return queue;
    }



    //对交换机进行测试
    @Test
    void testExchange(){
        Exchange textExchange = createTestExchange("textExchange");
        dataCenter.insertExchange(textExchange);
        Exchange exchange = dataCenter.getExchange(textExchange.getName());
        Assertions.assertEquals(textExchange, exchange);
        dataCenter.deleteExchange(textExchange.getName());
        Assertions.assertNull(dataCenter.getExchange(textExchange.getName()));
    }
    //对队列进行测试
    @Test
    void testQueue(){
        MSGQueue textQueue = createTestQueue("textQueue");
        dataCenter.insertQueue(textQueue);
        MSGQueue queue = dataCenter.getQueue(textQueue.getName());
        Assertions.assertEquals(textQueue, queue);
        dataCenter.deleteQueue(textQueue.getName());
        Assertions.assertNull(dataCenter.getQueue(textQueue.getName()));

    }
    @Test
    void testBinding() throws MqException {
        Binding binding = new Binding();
        binding.setExchangeName("textExchange");
        binding.setQueueName("textQueue");
        binding.setBindingKey("textBindingKey");
        dataCenter.insertBinding(binding);
        Binding binding1 = dataCenter.getBinding("textExchange", "textQueue");
        Assertions.assertEquals(binding, binding1);
        ConcurrentHashMap<String, Binding> bindingMap = dataCenter.getBindings("textExchange");
        Assertions.assertEquals(binding, bindingMap.get("textQueue"));
        Assertions.assertEquals(1, bindingMap.size());
        dataCenter.deleteBinding(binding);
        Assertions.assertNull(dataCenter.getBinding("textExchange", "textQueue"));
    }

    private Message createTestMessage(String context){
        return Message.createMessageId("testRoutingkey",null, context.getBytes());
    }
    @Test
    void testMessage()  {
        Message message = createTestMessage("textMessage");
        dataCenter.addMessage(message);
        Message message1 = dataCenter.getMessage(message.getMessageId());
        Assertions.assertEquals(message, message1);
        dataCenter.removeMessage(message.getMessageId());
        Assertions.assertNull(dataCenter.getMessage(message.getMessageId()));

    }
    @Test
    void testSendMessage() throws MqException {
        MSGQueue messageQueue = createTestQueue("textQueue");
        List<Message> messages = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            Message message = createTestMessage("textMessage" + i);
            messages.add(message);
            dataCenter.sendMessage(messageQueue, message);
        }
        int count = dataCenter.getMessageCount("textQueue");
        Assertions.assertEquals(10, count);
        for(Message message : messages){
            Message message1 = dataCenter.pollQueueMessages("textQueue");
            Assertions.assertEquals(message, message1);
        }
    }

    @Test
    void textWaitACK(){
        MSGQueue messageQueue = createTestQueue("textQueue");
        Message message = createTestMessage("textMessage");
        dataCenter.addMessageWaitACK("textQueue",message);
        Message message1 = dataCenter.getMessageWaitACK("textQueue", message.getMessageId());
        Assertions.assertEquals(message, message1);
        dataCenter.removeMessageWaitACK("textQueue",message.getMessageId());
        Assertions.assertNull(dataCenter.getMessageWaitACK("textQueue",message.getMessageId()));
    }

    @Test
    public void testRecovery() throws IOException, MqException, ClassNotFoundException {
        // 由于后续需要进行数据库操作, 依赖 MyBatis. 就需要先启动 SpringApplication, 这样才能进行后续的数据库操作.
        Application.context = SpringApplication.run(Application.class);

        // 1. 在硬盘上构造好数据
        DiskDataCenter diskDataCenter = new DiskDataCenter();
        diskDataCenter.init();

        // 构造交换机
        Exchange expectedExchange = createTestExchange("testExchange");
        diskDataCenter.insertExchange(expectedExchange);

        // 构造队列
        MSGQueue expectedQueue = createTestQueue("testQueue");
        diskDataCenter.insertQueue(expectedQueue);

        // 构造绑定
        Binding expectedBinding = new Binding();
        expectedBinding.setExchangeName("testExchange");
        expectedBinding.setQueueName("testQueue");
        expectedBinding.setBindingKey("testBindingKey");
        diskDataCenter.insertBinding(expectedBinding);

        // 构造消息
        Message expectedMessage = createTestMessage("testContent");
        diskDataCenter.sendMessage(expectedQueue, expectedMessage);

        // 2. 执行恢复操作
        dataCenter.recovery(diskDataCenter);

        // 3. 对比结果
        Exchange actualExchange = dataCenter.getExchange("testExchange");
        Assertions.assertEquals(expectedExchange.getName(), actualExchange.getName());
        Assertions.assertEquals(expectedExchange.getType(), actualExchange.getType());
        Assertions.assertEquals(expectedExchange.isDurable(), actualExchange.isDurable());
        Assertions.assertEquals(expectedExchange.isAutoDelete(), actualExchange.isAutoDelete());

        MSGQueue actualQueue = dataCenter.getQueue("testQueue");
        Assertions.assertEquals(expectedQueue.getName(), actualQueue.getName());
        Assertions.assertEquals(expectedQueue.isDurable(), actualQueue.isDurable());
        Assertions.assertEquals(expectedQueue.isAutoDelete(), actualQueue.isAutoDelete());
        Assertions.assertEquals(expectedQueue.isExclusive(), actualQueue.isExclusive());

        Binding actualBinding = dataCenter.getBinding("testExchange", "testQueue");
        Assertions.assertEquals(expectedBinding.getExchangeName(), actualBinding.getExchangeName());
        Assertions.assertEquals(expectedBinding.getQueueName(), actualBinding.getQueueName());
        Assertions.assertEquals(expectedBinding.getBindingKey(), actualBinding.getBindingKey());

        Message actualMessage = dataCenter.pollQueueMessages("testQueue");
        Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
        Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
        Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
        Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());

        // 4. 清理硬盘的数据, 把整个 data 目录里的内容都删掉(包含了 meta.db 和 队列的目录).
        Application.context.close();
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
    }
}