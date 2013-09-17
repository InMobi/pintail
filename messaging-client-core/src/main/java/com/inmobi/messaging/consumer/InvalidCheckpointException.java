package com.inmobi.messaging.consumer;

public class InvalidCheckpointException extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;

  public InvalidCheckpointException() {
  }

  public InvalidCheckpointException(String exceptionMessage) {
    super(exceptionMessage);
  }

  public InvalidCheckpointException(String msg, Exception e) {
    super(msg, e);
  }
}
