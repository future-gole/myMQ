package com.doublez.mqserver.datacenter;

import com.doublez.Application;
import com.doublez.mqserver.core.Binding;
import com.doublez.mqserver.core.Exchange;
import com.doublez.mqserver.core.ExchangeType;
import com.doublez.mqserver.core.MSGQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DataBaseManagerTest {

    private DataBaseManager dataBaseManager = new DataBaseManager();

    //进行准备工作
    @BeforeEach
    void setUp() {
        Application.context = SpringApplication.run(Application.class);
        dataBaseManager.init();

    }
    //进行数据清理
    @AfterEach
    public void tearDown() {
        // 此处的 context 对象，持有MetaMapper示例对象，然而MetaMapper对象又打开了meta.db数据库
        // 在windows上面，如果如果文件被某一个程序打开，那么直接删除（操作）是不会成功的 (linux 不会）
        //所以先要把 context 进行 close 才能删除 db
        Application.context.close();
        dataBaseManager.deleteDB();;
    }
    @Test
    public void testInitTable() {
        List<Exchange> exchanges = dataBaseManager.selectAllExchange();
        List<Binding> bindings = dataBaseManager.selectAllBinding();
        List<MSGQueue> msgQueues = dataBaseManager.selectAllQueue();

        Assertions.assertEquals(1,exchanges.size());
        Assertions.assertEquals("default",exchanges.get(0).getName());
        Assertions.assertEquals(ExchangeType.DIRECT,exchanges.get(0).getType());
        Assertions.assertEquals(0,bindings.size());
        Assertions.assertEquals(0,msgQueues.size());
    }
    @Test
    void insertExchange() {
    }

    @Test
    void insertQueue() {
    }

    @Test
    void insertBinding() {
    }

    @Test
    void deleteQueue() {
    }

    @Test
    void deleteBinding() {
    }

    @Test
    void deleteExchange() {
    }
}