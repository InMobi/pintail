package com.inmobi.messaging;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback
 * will generally execute in the background I/O thread so it should be fast.
 */
public interface Callback {

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the message sent to the server has been acknowledged.
     *
     * @param result Used to indicate the user whether the message has been successfully delivered to the server or not
     */
    void onCompletion(CallbackResult result);

}
