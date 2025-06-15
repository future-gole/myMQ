package com.doublez.mqserver.datacenter;

import com.doublez.Application;
import com.doublez.mqserver.core.Binding;
import com.doublez.mqserver.core.Exchange;
import com.doublez.mqserver.core.ExchangeType;
import com.doublez.mqserver.core.MSGQueue;
import com.doublez.mqserver.mapper.MetaMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.util.List;
import java.util.Queue;

@Slf4j
public class DataBaseManager {
    //因为该类并没有交给sping管理，所以需要手动管理
    private MetaMapper mateMapper;

    //1. 对数据库操作进行初始化
    public void init(){
        mateMapper = Application.context.getBean(MetaMapper.class);
        //1.1 判断是否存在
        if(!checkDBExists()){
            //1.2 不存在创建
            //1.2.1 创建目录
            File dataDir = new File("./data");
            dataDir.mkdirs();
            createTbale();
            //1.2.1 插入数据
            createDefaultData();
            log.info("DataBaseManager 数据库初始化完成");
        }else {
            //1.3 已经存在
            log.info("DataBaseManager 已经存在");
        }
    }

    private void createDefaultData() {
        Exchange exchange = new Exchange();
        exchange.setName("default");
        exchange.setType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        mateMapper.insertExchange(exchange);
        log.info("DataBaseManager 初始化数据完成 ");

    }
    //不需要手动创建mate.db，调用的时候mybatis会自动创建
    private void createTbale() {
        mateMapper.createQueueTable();
        mateMapper.createExchangeTable();
        mateMapper.createBindingTable();
        log.info("DataBaseManager 创建表完成");
    }

    private boolean checkDBExists() {
        File file = new File("./data/meta.db");
        return file.exists();
    }
    public void deleteDB(){
        File file = new File("./data/meta.db");
        if(file.delete()){
            log.info("DataBaseManager 数据库删除成功");
        }else {
            log.info("DataBaseManager 数据库删除失败");
        }

        File dir = new File("./data");
        if(dir.delete()){
            log.info("DataBaseManager 数据库目录删除成功");
        }else {
            log.info("DataBaseManager 数据库目录删除失败");
        }
    }

    public void insertExchange(Exchange exchange) {
        mateMapper.insertExchange(exchange);
    }
    public void insertQueue(MSGQueue queue) {
        mateMapper.insertQueue(queue);
    }
    public void insertBinding(Binding binding) {
        mateMapper.insertBinding(binding);
    }
    public void deleteQueue(String queueName) {
        mateMapper.deleteQueue(queueName);
    }
    public void deleteBinding(Binding binding) {
        mateMapper.deleteBinding(binding);
    }
    public void deleteExchange(String exchangeName) {
        mateMapper.deleteExchange(exchangeName);
    }

    public List<Exchange> selectAllExchange() {
        return mateMapper.selectExchanges();
    }
    public List<Binding> selectAllBinding() {
        return mateMapper.selectBindings();
    }
    public List<MSGQueue> selectAllQueue() {
        return mateMapper.selectQueues();
    }
}
