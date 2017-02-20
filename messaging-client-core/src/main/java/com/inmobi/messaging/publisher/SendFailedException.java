package com.inmobi.messaging.publisher;

public class SendFailedException extends PintailException {

    public SendFailedException(String msg) {
        super(msg);
    }

    public SendFailedException(String msg, Throwable th) {
        super(msg, th);
    }
}