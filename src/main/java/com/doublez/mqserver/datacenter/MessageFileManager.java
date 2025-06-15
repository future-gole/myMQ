package com.doublez.mqserver.datacenter;

import com.doublez.common.BinaryTool;
import com.doublez.common.MqException;
import com.doublez.mqserver.core.MSGQueue;
import com.doublez.mqserver.core.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

@Slf4j
public class MessageFileManager {
    //用内部静态来来存储队列的统计信息
    public static class Stat{
        public int totalCount;//总消息数量
        public int validCount;//有效消息数量
    }

    public void init(){
        //目前不需要初始化，留一个接口用于之后的扩展
    }
    // 1. 预定消息所在的目录和文件夹
    // 1. 获取指定消息队列的所在文件夹
    private String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }

    // 2. 获取 消息队列所属的消息数据文件路径 data.txt/.bin
    private String getQueueDataPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data.txt";
    }

    //3. 获取 消息队列所属的消息统计文件路径
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "/queue_stat.txt";
    }
    //4. 读取统计文件内容
    private Stat readStat(String queueName) throws MqException {
        Stat stat = new Stat();
        try(InputStream in = new FileInputStream(getQueueStatPath(queueName))) {
            Scanner scanner = new Scanner(in);
            stat.totalCount = scanner.nextInt();
            stat.validCount = scanner.nextInt();
            return stat;
        } catch (IOException e) {
            log.error("读取stat文件错误", e);
            throw new MqException("[MessageFileManager] 读取stat文件错误, stat:"+getQueueStatPath(queueName));
        }
    }
    //5. 写入统计文件内容，覆盖写入，如果不覆盖需要在FileOutputStream中添加true参数
    private void writeStat(String queueName, Stat stat) throws MqException {
        try(OutputStream out = new FileOutputStream(getQueueStatPath(queueName))) {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
            bw.write(stat.totalCount + "\t" + stat.validCount );
            //写入文件！！
            bw.flush();
        } catch (IOException e) {
            log.error("写入stat文件错误", e);
            throw new MqException("[MessageFileManager] 写入stat文件错误, stat:"+getQueueStatPath(queueName));
        }
    }

    //6. 创建队列对应的文件和目录
    public void createQueueFiles(String queueName) throws IOException, MqException {
        //6.1 创建队列对应的目录
        File file = new File(getQueueDir(queueName));
        if(!file.exists()) {
            boolean ok = file.mkdirs();
            if(!ok){
                throw new IOException("[MessageFileManager] 创建目录失败! dirs:"+file.getAbsolutePath());
            }
        }
        //6.2 创建队列数据文件
        File DataQueuefile = new File(getQueueDataPath(queueName));
        if(!DataQueuefile.exists()){
            boolean DataOk = DataQueuefile.createNewFile();
            if(!DataOk){
                throw new IOException("[MessageFileManager] 创建数据文件失败! file:"+DataQueuefile.getAbsolutePath());
            }
        }
        //6.3 创建队列消息统计文件
        File statQueuefile = new File(getQueueStatPath(queueName));
        if(!statQueuefile.exists()){
            boolean StatOk = statQueuefile.createNewFile();
            if(!StatOk){
                throw new IOException("[MessageFileManager] 创建消息统计文件失败! file:"+statQueuefile.getAbsolutePath());
            }
        }

        //6.4 设置初始值
        Stat stat = new Stat();
        stat.totalCount = 0;
        stat.validCount = 0;
        writeStat(queueName, stat);
    }

    //7. 删除队列的目录和文件
    public void destroyQueueFiles(String queueName) throws IOException {
        File dataQueuefile = new File(getQueueDataPath(queueName));
        boolean ok1 = dataQueuefile.delete();
        File statQueuefile = new File(getQueueStatPath(queueName));
        boolean ok2 = statQueuefile.delete();
        File QueueDirFile = new File(getQueueDir(queueName));
        boolean ok3 = QueueDirFile.delete();
        if(!ok1 || !ok2 || !ok3){
            throw new IOException("[MessageFileManager] 删除队列目录和文件失败！file:"+QueueDirFile.getAbsolutePath());
        }
    }
    //8. 检查队列的目录和文件是否存在
    //用来判断生成者给broker server 生成消息，这个消息可能就需要被记录到文件上（取决消息是否要持久化）
    public boolean checkFilesExists(String queueName) {
        File statQueuefile = new File(getQueueStatPath(queueName));
        if(!statQueuefile.exists()){
            return false;
        }
        File dataQueuefile = new File(getQueueDataPath(queueName));
        return dataQueuefile.exists();
    }

    //9. 把一个消息放到队列的对应文件中
    public void sendMessage( MSGQueue queue, Message message) throws MqException, IOException {
        //1. 先判断是否存在
        if(!checkFilesExists(queue.getName())){
            throw new MqException("[MessageFileManager] 队列消息文件不存在,queueName:"+ queue.getName());
        }
        //2. 将对象序列化转成byte数组
        byte[] messageBites = BinaryTool.toBytes(message);
        //防止多线程造成的写入错误，需要加锁
        synchronized (queue) {
            //3. 获取当前队列数据文件的长度，计算Message的对象offsetBeg 和 offsetEnd
            //offsetBeg = length + 4;
            //offsetEnd = length + 4 + message;
            File dataQueuefile = new File(getQueueDataPath(queue.getName()));
            message.setOffsetBeg(dataQueuefile.length() + 4);
            message.setOffsetEnd(dataQueuefile.length() + 4 + messageBites.length);
            //4. 写入消息到数据文件,需要追加到文件末尾
            try(OutputStream out = new FileOutputStream(dataQueuefile, true)) {
                try (DataOutputStream dos = new DataOutputStream(out)) {
                    //4.1 先写入当前消息的长度，4个字节
                    dos.writeInt(messageBites.length);
                    //4.2 写入消息内容
                    dos.write(messageBites);
                }
            }
            //5. 更新消息统计文件
            try {
                Stat stat = readStat(queue.getName());
                stat.totalCount++;
                stat.validCount++;
                writeStat(queue.getName(), stat);
            } catch (MqException e) {
                log.error("数据已经写入data文件，但是stat文件写入失败，stat:{},导致文件数据不一致", queue.getName());
                throw new IOException("[MessageFileManager] 更新stat文件失败"+queue.getName(),e);
            }
        }
    }
    //10. 删除消息方法，逻辑删除 isValid 设置为0
    public void deleteMessage(MSGQueue queue, Message message) throws MqException, IOException, ClassNotFoundException {
        synchronized (queue) {
            //更新消息文件
            try(RandomAccessFile raf = new RandomAccessFile(getQueueDataPath(queue.getName()), "rw")) {
                //1. 读取对应的message数据
                byte[] bufferSrc = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                raf.seek(message.getOffsetBeg());//指定光标位置
                raf.read(bufferSrc);//读取
                //2. 把读出的raf二进制对象转化为message对象
                Message diskMsg = (Message) BinaryTool.fromBytes(bufferSrc);
                //3. 把isValid设置为无效（此时设置的是硬盘中的message对象，内存中的不需要修改，因为会被自动销毁）
                diskMsg.setIsValid((byte) 0x0);
                //4. 对象转化为二进制重新写入文件
                byte[] bufferDest = BinaryTool.toBytes(diskMsg);
                //4.1 检查长度是否相同
                if (bufferDest.length != bufferSrc.length) {
                    // 你需要决定如何处理这种情况。
                    // 选项1: 抛出异常，因为这违反了原地更新的假设
                    throw new MqException("Error: 更新后的大小 (" + bufferDest.length +
                            ") 与原先大小不匹配 (" + bufferSrc.length +
                            ") 消息起使位置 " + message.getOffsetBeg());
                }
                //4.1 重新seek，因为上述seek之后会导致光标变化
                raf.seek(message.getOffsetBeg());
                raf.write(bufferDest);
            }
            //更新统计文件
            Stat stat = readStat(queue.getName());
            if(stat.validCount > 0){
                stat.validCount--;
            }
            writeStat(queue.getName(), stat);
        }
    }
    //11. 从文件中提取所有的消息内容，加载到内存中
    public LinkedList<Message> loadMessageFromQueue(String queueName) throws MqException, IOException, ClassNotFoundException {
        //该方法再程序启动的时候进行调用，不涉及多线程，不需要加锁
        LinkedList<Message> messages = new LinkedList<>();
        try(InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))){
            try(DataInputStream dis = new DataInputStream(inputStream)){
                //定义光标
                int currentOffset = 0;
                while(true){
                    //1. 读取当前消息长度
                    //DataInputStream的readInt如果读到文件末尾会抛出EOFException异常，直接跳出循环
                    int messageSize = dis.readInt();
                    //2. 读取消息内容
                    byte[] buffer = new byte[messageSize];
                    int actuallySize = dis.read(buffer);
                    if(actuallySize != messageSize){
                        throw new MqException("[MessageFileManager] 读取文件格式错误！queueName:"+queueName);
                    }
                    //3. 把二进制转化为message对象
                    Message message = (Message) BinaryTool.fromBytes(buffer);
                    //4. 判断是否是无效数据
                    if(message.getIsValid() != 0x1){
                        //光标变化
                        currentOffset += (4 + messageSize);
                        continue;
                    }
                    //5. 有效数据,加入队列
                    //5.1 手动计算光标位置（DataInputStream 没有seek方法）
                    message.setOffsetBeg(currentOffset + 4);
                    message.setOffsetEnd(currentOffset + 4 + messageSize);
                    //5.2 改变光标
                    currentOffset += (4 + messageSize);
                    messages.add(message);
                }
            }catch (EOFException e){
                log.info("[MessageFileManager] 恢复Message 数据完成!");
            }
        }
        return messages;
    }
    //12. 判断是否要对当前队列的消息文件进行GC
    public boolean checkGC(String queueName) throws MqException {
        //读取Stat文件
        Stat stat = readStat(queueName);
        //自定义判断方法：总量大于2000，有效量小于0.5
        return stat.totalCount > 2000 && (double) stat.validCount / (double) stat.totalCount < 0.5;
    }
    //13. 创建新的文件
    private String getQueueDataNewPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data_new.txt";
    }
    //14. 对消息数据文件进行gc
    public void gc(MSGQueue queue) throws MqException, IOException, ClassNotFoundException {
        synchronized (queue) {
            // 1. 记录gc耗时
            long startTime = System.currentTimeMillis();
            //1. 创建新的文件
            File queueDataNewfile = new File(getQueueDataNewPath(queue.getName()));
///            boolean ok = queueDataNewfile.mkdirs(); //这个是创建目录不是而不是创建文件！！
            //1.1 不应该存在，存在说明上次gc意外退出，应该报错
            if(queueDataNewfile.exists()){
                throw new MqException("[MessageFileManager] gc时发现 queue_data_new 已经存在：queueDataNewFiles:"+queueDataNewfile.getName());
            }
            if(!queueDataNewfile.createNewFile()){
                throw new MqException("[MessageFileManager] 创建文件失败：queueDataNewFiles:"+queueDataNewfile.getName());
            }
            //2. 从原来的文件中读取所有有用的消息对象
            LinkedList<Message> messages = loadMessageFromQueue(queue.getName());
            //3. 把有效消息直接写入文件
            /// 这里不能是queueDataNewFile.getName()
            /// 因为这样是在当前工作目录（通常是项目根目录）下创建并打开一个名为 "queue_data_new.txt" 的文件进行写入。
            try(OutputStream out = new FileOutputStream(queueDataNewfile)) {
                try(DataOutputStream dos = new DataOutputStream(out)) {
                    for(Message message : messages){
                        //3.1 写入队列的消息文件
                        //3.1.1 转化为二进制对象
                        byte[] messageBitys = BinaryTool.toBytes(message);
                        //3.1.2 写入文件
                        dos.writeInt(messageBitys.length);
                        dos.write(messageBitys);
                    }
                }
            }
            //4. 删除旧的数据文件，并且把新的数据文件进行重命名
            File queueDataOldFile = new File(getQueueDataPath(queue.getName()));
            boolean ok = queueDataOldFile.delete();
            if(!ok){
                throw new MqException("[MessageFileManager] 删除旧的文件失败！queueDataOldFiles:"+queueDataOldFile.getAbsolutePath());
            }
            ok = queueDataNewfile.renameTo(queueDataOldFile);
            if(!ok){
                throw new MqException("[MessageFileManager] 文件重命名失败 queueDataNewFiles:"+queueDataNewfile.getAbsolutePath() +
                        "queueDataOldFiles:"+queueDataOldFile.getAbsolutePath());
            }
            //5. 更新消息统计文件
            try {
                Stat stat = readStat(queue.getName());
                stat.totalCount = messages.size();
                stat.validCount = messages.size();;
                writeStat(queue.getName(), stat);
            } catch (MqException e) {
                log.error("数据已经写入data文件，但是stat文件写入失败，stat:{},导致文件数据不一致", queue.getName());
                throw new IOException("[MessageFileManager] 更新stat文件失败"+queue.getName(),e);
            }
            long endTime = System.currentTimeMillis();
            log.info("[MessageFileManager] gc 消耗时间：{} ms", (endTime - startTime));
        }
    }
}
