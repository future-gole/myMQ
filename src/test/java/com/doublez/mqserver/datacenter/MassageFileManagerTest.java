package com.doublez.mqserver.datacenter;

import com.doublez.common.BinaryTool;
import com.doublez.common.MqException;
import com.doublez.mqserver.core.MSGQueue;
import com.doublez.mqserver.core.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@SpringBootTest
class MassageFileManagerTest {

    private MessageFileManager messageFileManager = new MessageFileManager();
    private static final String queueName1 = "queue1";
    private static final String queueName2 = "queue2";
    @BeforeEach
    void setUp() throws IOException, MqException {
        messageFileManager.createQueueFiles(queueName1);
        messageFileManager.createQueueFiles(queueName2);
    }
    @AfterEach
    void teatOut() throws IOException {
        messageFileManager.destroyQueueFiles(queueName1);
        messageFileManager.destroyQueueFiles(queueName2);
    }
    @Test
    public void testCreateFiles(){
        File queueDataFile1 = new File("./data" + File.separator + queueName1 + File.separator + "queue_data.txt");
        Assertions.assertTrue(queueDataFile1.isFile());
        File queueStatFile1 = new File("./data" + File.separator + queueName1 + File.separator + "queue_stat.txt");
        Assertions.assertTrue(queueStatFile1.isFile());
        File queueDataFile2 = new File("./data" + File.separator + queueName2 + File.separator + "queue_data.txt");
        Assertions.assertTrue(queueDataFile2.isFile());
        File queueStatFile2 = new File("./data" + File.separator + queueName2 + File.separator + "queue_stat.txt");
        Assertions.assertTrue(queueStatFile2.isFile());
    }
    @Test
    public void testReadWriteStat() throws IOException, MqException {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.validCount = 50;
        stat.totalCount = 100;
        //通过spring提供的反射获取私有的方法来进行测试
        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", queueName1,stat);
        MessageFileManager.Stat readStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        Assertions.assertEquals(50,readStat.validCount);
        Assertions.assertEquals(100,readStat.totalCount);
    }

    private Message createMessage(String content) throws IOException {
        byte[] contentBytes = BinaryTool.toBytes(content);
        return Message.createMessageId("testRoutingKey", null, contentBytes);
    }
    private MSGQueue createQueue(String queueName) throws IOException, MqException {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(false);
        return queue;
    }
    @Test
    public void testSendMessage() throws IOException, MqException, ClassNotFoundException {
        //1. 创建消息
        Message message = createMessage("testMessage");
        //2. 创建队列
        MSGQueue queue = createQueue(queueName1);
        //3. 发送消息
        messageFileManager.sendMessage(queue, message);

        //4. 检查txt文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        Assertions.assertEquals(1,stat.validCount);
        Assertions.assertEquals(1,stat.totalCount);

        //5. 检查data文件
        LinkedList<Message> messages = messageFileManager.loadMessageFromQueue(queueName1);
        Message msg = messages.get(0);
        Assertions.assertEquals(message.getMessageId(),msg.getMessageId());
        Assertions.assertEquals(message.getRoutingKey(),message.getRoutingKey());
        Assertions.assertEquals(message.getDeliveryMode(),msg.getDeliveryMode());
        Assertions.assertArrayEquals(message.getBody(),msg.getBody());
    }
    @Test
    public void testLoadMessageFromQueue() throws IOException, MqException, ClassNotFoundException {
        //1. 存储原先消息
        List<Message> expectMessages = new LinkedList<>();
        //2. 创建队列
        MSGQueue queue = createQueue(queueName1);
        //3. 模拟大量消息
        for(int i = 0; i < 100; i++){
            Message msg = createMessage("testMessage, i:" + i);
            messageFileManager.sendMessage(queue, msg);
            expectMessages.add(msg);
        }
        //4. 读取消息
        LinkedList<Message> actualMessages = messageFileManager.loadMessageFromQueue(queueName1);
        Assertions.assertEquals(actualMessages.size(),expectMessages.size());
        for(int i = 0; i < expectMessages.size(); i++){
            Assertions.assertEquals(expectMessages.get(i).getMessageId(),actualMessages.get(i).getMessageId());
            Assertions.assertEquals(expectMessages.get(i).getRoutingKey(),actualMessages.get(i).getRoutingKey());
            Assertions.assertEquals(expectMessages.get(i).getDeliveryMode(),actualMessages.get(i).getDeliveryMode());
            Assertions.assertArrayEquals(expectMessages.get(i).getBody(),actualMessages.get(i).getBody());
            Assertions.assertEquals(0x1,actualMessages.get(i).getIsValid());
        }
    }
    @Test
    public void testDeleteMessage() throws IOException, MqException, ClassNotFoundException {
        //1. 存储原先消息
        List<Message> expectMessages = new LinkedList<>();
        //2. 创建队列
        MSGQueue queue = createQueue(queueName1);
        //3. 模拟大量消息
        for(int i = 0; i < 100; i++){
            Message msg = createMessage("testMessage, i:" + i);
            messageFileManager.sendMessage(queue, msg);
            expectMessages.add(msg);
        }

        //4. 删除消息
        messageFileManager.deleteMessage(queue,expectMessages.get(99));
        messageFileManager.deleteMessage(queue,expectMessages.get(98));
        messageFileManager.deleteMessage(queue,expectMessages.get(97));
        //4. 读取消息
        LinkedList<Message> actualMessages = messageFileManager.loadMessageFromQueue(queueName1);
        Assertions.assertEquals(97,actualMessages.size());
        for(int i = 0; i < actualMessages.size(); i++){
            Assertions.assertEquals(expectMessages.get(i).getMessageId(),actualMessages.get(i).getMessageId());
            Assertions.assertEquals(expectMessages.get(i).getRoutingKey(),actualMessages.get(i).getRoutingKey());
            Assertions.assertEquals(expectMessages.get(i).getDeliveryMode(),actualMessages.get(i).getDeliveryMode());
            Assertions.assertArrayEquals(expectMessages.get(i).getBody(),actualMessages.get(i).getBody());
            Assertions.assertEquals(0x1,actualMessages.get(i).getIsValid());
        }
    }

    @Test
    public void testGC() throws IOException, MqException, ClassNotFoundException {
        List<Message> expectMessages = new LinkedList<>();
        MSGQueue queue = createQueue(queueName1);
        for(int i = 0; i < 2010; i++){
            Message msg = createMessage("testMessage, i:" + i);
            messageFileManager.sendMessage(queue, msg);
            expectMessages.add(msg);
        }
        //GC之前的文件大小
        File beforeGC = new File("./data/"+queueName1+"/queue_data.txt");
        long beforeGCLength = beforeGC.length();

        for(int i = 10; i < 2000; i++){
            messageFileManager.deleteMessage(queue,expectMessages.get(i));
        }
        if(messageFileManager.checkGC(queueName1)){
            System.out.println("开始GC回收");
            messageFileManager.gc(queue);
        }

        LinkedList<Message> messages = messageFileManager.loadMessageFromQueue(queueName1);
        Assertions.assertEquals(20,messages.size());
        //GC之后文件大小
        File afterGC = new File("./data/"+queueName1+"/queue_data.txt");
        long afterGCLength = afterGC.length();
        System.out.println("Before GCLength: "+beforeGCLength);
        System.out.println("After GCLength: "+afterGCLength);
        Assertions.assertTrue(beforeGCLength > afterGCLength);
    }
}