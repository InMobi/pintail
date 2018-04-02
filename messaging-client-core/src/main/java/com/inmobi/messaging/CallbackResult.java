package com.inmobi.messaging;

/**
 * This is to notify the user whether the acknowledgement has been received from the server or not.
 */
public class CallbackResult {

    private Result result;

    public CallbackResult(Result result) {
        this.result = result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public Result getResult() {
        return result;
    }

    public enum Result {
        SUCCESS, FAILURE;
    }

}
