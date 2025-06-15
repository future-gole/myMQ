package com.doublez.mqclinet;

import lombok.Data;

import java.io.IOException;
@Data
public class ConnectionFactory {
    private String host;

    private int port;

    private String virtualHostName;

    public Connection newConnection() throws IOException {
        Connection con = new Connection(host,port);
        return con;
    }
}
