package com.inmobi.messaging.consumer;

public class EndOfStreamException extends Exception {

  public EndOfStreamException() {
  }

  public EndOfStreamException(String exceptionMessage) {
    super(exceptionMessage);
  }
}
