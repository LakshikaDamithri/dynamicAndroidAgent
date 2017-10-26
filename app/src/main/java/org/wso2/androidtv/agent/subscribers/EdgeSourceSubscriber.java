package org.wso2.androidtv.agent.subscribers;


import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

/**
 * Created by gathikaratnayaka on 10/17/17.
 */

public class EdgeSourceSubscriber {

    private SourceEventListener sourceEventListener;
    private String id;


    public EdgeSourceSubscriber(SourceEventListener sourceEventListener, String id){
        this.sourceEventListener = sourceEventListener;
        this.id = id;
    }

    public void recieveEvent(String message, String[] strings){
        sourceEventListener.onEvent(message,strings);
    }
}
