package com.inmobi.messaging;

public class PintailException extends Exception {

    public PintailException(String msg) {
        super(msg);
    }

    public PintailException(String msg, Throwable th) {
        super(msg, th);
    }

}