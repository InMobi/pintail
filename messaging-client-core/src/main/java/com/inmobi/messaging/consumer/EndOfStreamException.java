package com.inmobi.messaging.consumer;

public class EndOfStreamException extends Exception {

  private static final long serialVersionUID = 1L;

  public EndOfStreamException() {
  }

  public EndOfStreamException(String exceptionMessage) {
    super(exceptionMessage);
  }
}
