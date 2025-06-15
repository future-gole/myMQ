package com.doublez.mqclinet;

import com.doublez.common.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Data
@AllArgsConstructor
public class Connection {
    //socket对象
    private Socket socket;
    //map来管理多个channel对象
    private ConcurrentHashMap<String,Channel> channelMap = new ConcurrentHashMap<>();

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    private ExecutorService callbackPool = null;

    public Connection(String host, int port) throws IOException {
        socket  = new Socket(host,port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);

        callbackPool = Executors.newFixedThreadPool(4);
        //扫描线程
        Thread thread = new Thread(() -> {
            try{
                while(!socket.isClosed()){
                    Response response = readResponse();
                    dispatchResponse(response);
                }
            }catch (SocketException e){
                log.info("socket正常关闭！");
            } catch (IOException | ClassNotFoundException | MqException e) {
                log.error("socket 连接异常断开,e:{}",e.getMessage());
            }
        });

        thread.start();
    }

    public void close() {
        // 关闭 Connection 释放上述资源
        try {
            callbackPool.shutdownNow();
            channelMap.clear();
            inputStream.close();
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 使用这个方法来分别处理, 当前的响应是一个针对控制请求的响应, 还是服务器推送的消息.
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, MqException {
        if (response.getType() == 0xc) {
            // 服务器推送来的消息数据
            SubScribeReturns subScribeReturns = (SubScribeReturns) BinaryTool.fromBytes(response.getPayload());
            // 根据 channelId 找到对应的 channel 对象
            Channel channel = channelMap.get(subScribeReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 该消息对应的 channel 在客户端中不存在! channelId=" + subScribeReturns.getChannelId());
            }
            // 执行该 channel 对象内部的回调.
            callbackPool.submit(() -> {
                try {
                    channel.getConsumer().handleDelivery(subScribeReturns.getConsumerTag(), subScribeReturns.getBasicProperties(),
                            subScribeReturns.getBody());
                } catch (MqException | IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
            // 当前响应是针对刚才的控制请求的响应
            BasicReturns basicReturns = (BasicReturns) BinaryTool.fromBytes(response.getPayload());
            // 把这个结果放到对应的 channel 的 hash 表中.
            Channel channel = channelMap.get(basicReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 该消息对应的 channel 在客户端中不存在! channelId=" + basicReturns.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }

    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
        log.info("发送请求成功，type:{}, length:{}",request.getType(),request.getLength());
    }

    public Response readResponse() throws IOException {
        Response response = new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload = new byte[response.getLength()];
        int read = dataInputStream.read(payload);
        if(read != response.getLength()){
            throw new IOException("读取响应格式不正确");
        }
        response.setPayload(payload);
        log.info("收到响应！ type:{}, length:{}",response.getType(),response.getLength());
        return response;
    }

    //在connection中创建一个channel
    public Channel createChannel() throws IOException {
        String channelId = "C-" + UUID.randomUUID();
        Channel channel = new Channel(channelId,this);
        //通知服务器创建channel了
        //必须要先创建！！
        channelMap.put(channelId,channel);
        boolean ok = channel.createChannel();
        if(!ok){
            log.error("channel 创建失败");
            channelMap.remove(channelId);
            return null;
        }

        log.info("channel 创建成功");
        return channel;
    }
}
