package com.doublez.common;

import java.io.*;

//对象需要实现序列化和反序列号需要实现Serializable
public class BinaryTool {
    //把一个对象序列化成为一个字节数组
    public static byte[] toBytes(Object object) throws IOException {
        //因为没有办法判断对象的长度，所以需要使用ByteArrayOutputStream来创建可变数组
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(object);//写入的是bytes流
            }
            return bytes.toByteArray();
        }
    }
    //把一个字节数组序列化为一个对象
    public static Object fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            //readObject 就是从byte[]中读取数据进行反序列化
            return in.readObject();
        }
    }
}
