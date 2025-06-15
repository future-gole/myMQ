package com.doublez.mqserver.core;

public enum ExchangeType {
    DIRECT(0),
    FANOUT(1),
    TOPIC(2);

    private final int type;

    private ExchangeType(int type){
        this.type = type;
    }
    public int getType(){
        return type;
    }
}
