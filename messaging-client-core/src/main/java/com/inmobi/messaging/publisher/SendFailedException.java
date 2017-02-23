package com.inmobi.messaging.publisher;

import com.inmobi.messaging.PintailException;

public class SendFailedException extends PintailException {

    public SendFailedException(String msg) {
        super(msg);
    }

    public SendFailedException(String msg, Throwable th) {
        super(msg, th);
    }
}