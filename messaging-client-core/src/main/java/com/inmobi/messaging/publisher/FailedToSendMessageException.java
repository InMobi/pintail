package com.inmobi.messaging.publisher;

public class FailedToSendMessageException extends PintailException {

    public FailedToSendMessageException(String msg) {
        super(msg);
    }

    public FailedToSendMessageException(String msg, Throwable th) {
        super(msg, th);
    }
}
