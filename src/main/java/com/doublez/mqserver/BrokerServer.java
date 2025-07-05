package com.doublez.mqserver;

import com.doublez.common.*;
import com.doublez.mqserver.core.BasicProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * 这个 BrokerServer 就是咱们 消息队列 本体服务器.
 * 本质上就是一个 TCP 的服务器.
 */
@Slf4j
public class BrokerServer {
    //socket
    private ServerSocket serverSocket = null;
    //virtualHost,当前只考虑有一个虚拟主机
    private VirtualHost virtualHost = new VirtualHost("default");
    //key: channelId, value: socket对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<>();
    //创建线程池
    private ExecutorService executorService = null;
    //控制是否持续运行
    private volatile boolean runnable = true;

    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void start() throws IOException {
        log.info("BrokerServer started");
        executorService = Executors.newCachedThreadPool();
        try {
            while(runnable) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> {
                    progressConnection(clientSocket);
                });
            }
        } catch (SocketException e) {
            log.info("服务器停止运行");
        }
    }

    public void stop() throws IOException {
        runnable = false;
        //销毁线程
        executorService.shutdown();
        serverSocket.close();
    }
    //处理客户端的连接
    private void progressConnection(Socket clientSocket) {
        try(InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream()) {
            try(DataInputStream dataInputStream = new DataInputStream(inputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {

                while(true){
                    //1. 读取请求并解析
                    Request request  = readRequest(dataInputStream);
                    //2. 处理并计算响应
                    Response response = process(request,clientSocket);
                    //3. 写回响应给客户端
                    writeResponse(dataOutputStream,response);
                }
            }
        }catch (EOFException |SocketException e){
            //正常退出
            log.info("connection closed! 客户端地址: {}, 端口: {}"
                    ,clientSocket.getInetAddress(),clientSocket.getPort());
        } catch (IOException | ClassNotFoundException | MqException e) {
            log.error("connection 异常退出，e:{}",e.getMessage());
            e.printStackTrace();
        }finally {
            try {
                clientSocket.close();
                clearClosedSession(clientSocket);
            } catch (IOException e) {
                log.error("connection close error,e: {}",e.getMessage());
            }
        }
    }

    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        //刷新缓冲区
        dataOutputStream.flush();
    }

    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException, MqException {
        // 1. 把 request 中的 payload 做一个初步的解析.
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        System.out.println("[Request] rid=" + basicArguments.getRid() + ", channelId=" + basicArguments.getChannelId()
                + ", type=" + request.getType() + ", length=" + request.getLength());
        // 2. 根据 type 的值, 来进一步区分接下来这次请求要干啥.
        boolean ok = true;
        if (request.getType() == 0x1) {
            // 创建 channel
            sessions.put(basicArguments.getChannelId(), clientSocket);
            log.info("[BrokerServer] 创建 channel 完成! channelId= {}" , basicArguments.getChannelId());
        } else if (request.getType() == 0x2) {
            // 销毁 channel
            sessions.remove(basicArguments.getChannelId());
            System.out.println("[BrokerServer] 销毁 channel 完成! channelId=" + basicArguments.getChannelId());
        } else if (request.getType() == 0x3) {
            // 创建交换机. 此时 payload 就是 ExchangeDeclareArguments 对象了.
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            ok = virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeType(),
                    arguments.isDurable(), arguments.isAutoDelete(), arguments.getArguments());
        } else if (request.getType() == 0x4) {
            ExchangeDeleteArguments arguments = (ExchangeDeleteArguments) basicArguments;
            ok = virtualHost.exchangeDelete(arguments.getExchangeName());
        } else if (request.getType() == 0x5) {
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            ok = virtualHost.queueDeclare(arguments.getQueueName(), arguments.isDurable(),
                    arguments.isExclusive(), arguments.isAutoDelete(), arguments.getArguments());
        } else if (request.getType() == 0x6) {
            QueueDeleteArguments arguments = (QueueDeleteArguments) basicArguments;
            ok = virtualHost.queueDelete((arguments.getQueueName()));
        } else if (request.getType() == 0x7) {
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            ok = virtualHost.queueBind(arguments.getQueueName(), arguments.getExchangeName(), arguments.getBindingKey());
        } else if (request.getType() == 0x8) {
            QueueUnbindArguments arguments = (QueueUnbindArguments) basicArguments;
            ok = virtualHost.queueUnbind(arguments.getQueueName(), arguments.getExchangeName());
        } else if (request.getType() == 0x9) {
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            ok = virtualHost.basicPublish(arguments.getExchangeName(), arguments.getRoutingKey(),
                    arguments.getBasicProperties(), arguments.getBody());
        } else if (request.getType() == 0xa) {
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            ok = virtualHost.basicConsume(arguments.getConsumerTag(), arguments.getQueueName(), arguments.isAutoAck(),
                    new Consumer() {
                        // 这个回调函数要做的工作, 就是把服务器收到的消息可以直接推送回对应的消费者客户端
                        @Override
                        public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MqException, IOException {
                            // 先知道当前这个收到的消息, 要发给哪个客户端.
                            // 此处 consumerTag 其实是 channelId. 根据 channelId 去 sessions 中查询, 就可以得到对应的
                            // socket 对象了, 从而可以往里面发送数据了
                            // 1. 根据 channelId 找到 socket 对象
                            Socket clientSocket = sessions.get(consumerTag);
                            if (clientSocket == null || clientSocket.isClosed()) {
                                throw new MqException("[BrokerServer] 订阅消息的客户端已经关闭!");
                            }
                            // 2. 构造响应数据
                            SubScribeReturns subScribeReturns = new SubScribeReturns();
                            subScribeReturns.setChannelId(consumerTag);
                            subScribeReturns.setRid(""); // 由于这里只有响应, 没有请求, 不需要去对应. rid 暂时不需要.
                            subScribeReturns.setOk(true);
                            subScribeReturns.setConsumerTag(consumerTag);
                            subScribeReturns.setBasicProperties(basicProperties);
                            subScribeReturns.setBody(body);
                            byte[] payload = BinaryTool.toBytes(subScribeReturns);
                            Response response = new Response();
                            // 0xc 表示服务器给消费者客户端推送的消息数据.
                            response.setType(0xc);
                            // response 的 payload 就是一个 SubScribeReturns
                            response.setLength(payload.length);
                            response.setPayload(payload);
                            // 3. 把数据写回给客户端.
                            //    注意! 此处的 dataOutputStream 这个对象不能 close !!!
                            //    如果 把 dataOutputStream 关闭, 就会直接把 clientSocket 里的 outputStream 也关了.
                            //    此时就无法继续往 socket 中写入后续数据了.
                            DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
                            writeResponse(dataOutputStream, response);
                        }
                    });
        } else if (request.getType() == 0xb) {
            // 调用 basicAck 确认消息.
            BasicAckArguments arguments = (BasicAckArguments) basicArguments;
            ok = virtualHost.basicAck(arguments.getQueueName(), arguments.getMessageId());
        } else {
            // 当前的 type 是非法的.
            throw new MqException("[BrokerServer] 未知的 type! type=" + request.getType());
        }
        // 3. 构造响应
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setOk(ok);
        byte[] payload = BinaryTool.toBytes(basicReturns);
        Response response = new Response();
        response.setType(request.getType());
        response.setLength(payload.length);
        response.setPayload(payload);
        log.info("[Response] rid=" + basicReturns.getRid() + ", channelId=" + basicReturns.getChannelId()
                + ", type=" + response.getType() + ", length=" + response.getLength());
        return response;
    }


    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] playload = new byte[request.getLength()];
        int n  = dataInputStream.read(playload);
        if(n != request.getLength()){
            throw new IOException("读取请求格式错误");
        }
        request.setPayload(playload);
        return request;
    }

    private void clearClosedSession(Socket clientSocket) {
        List<String> TodeleteChannelId = new ArrayList<String>();
        for(Map.Entry<String,Socket> entry : sessions.entrySet()){
            if(entry.getValue() == clientSocket){
                TodeleteChannelId.add(entry.getKey());
            }
        }
        for(String channelId : TodeleteChannelId){
            sessions.remove(channelId);
        }
        log.info("清理 session 完成！ channelId:{}" , TodeleteChannelId);
    }
}
