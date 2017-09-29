package org.wso2.androidtv.agent.siddhiSinks;

import android.util.EventLog;
import android.util.Log;

import net.minidev.json.parser.JSONParser;

import org.json.JSONException;
import org.json.JSONObject;
import org.wso2.androidtv.agent.constants.TVConstants;
import org.wso2.androidtv.agent.mqtt.transport.TransportHandlerException;
import org.wso2.androidtv.agent.util.LocalRegistry;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.core.event.Event;

import java.util.Calendar;
import java.util.Map;

import org.wso2.androidtv.agent.mqtt.AndroidTVMQTTHandler;
import org.wso2.androidtv.agent.services.DeviceManagementService;
import org.wso2.androidtv.agent.services.CacheManagementService;

/**
 * Created by gathikaratnayaka on 9/29/17.
 */

@Extension(
        name = "edgeGateway",
        namespace = "sink",
        description = "This sink publishes data from edgeGateway to broker ",
        parameters = { @Parameter(
                name = "topic",
                description = "The topic to which the events processed by WSO2 SP are published via MQTT. " +
                        "This is a mandatory parameter.",
                type = {DataType.STRING},
                dynamic = true)},
        examples = @Example(description = "TBD", syntax = "TBD")
)

public class EdgeGatewaySink extends Sink {

    private static AndroidTVMQTTHandler androidTVMQTTHandler;

    private Option topicOption;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Event.class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{MqttConstants.MESSAGE_TOPIC};

    }

    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.topicOption = optionHolder.validateAndGetOption(MqttConstants.MESSAGE_TOPIC);
    }

    @Override
    public void publish(Object o, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {

        System.out.println("sinkOutput1 :" + o);
        try {
            JSONObject jObject = new JSONObject(o.toString());
            JSONObject event = jObject.getJSONObject("event");

            System.out.println("sinkJsonlength:" + event.length());


            JSONObject jsonEvent = new JSONObject();
            JSONObject jsonMetaData = new JSONObject();

            try {
                jsonMetaData.put("owner", LocalRegistry.getOwnerNameSiddhi());
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                jsonMetaData.put("deviceId", LocalRegistry.getDeviceIDSiddhi());
            } catch (JSONException e) {
                e.printStackTrace();
            }

            try {
                jsonMetaData.put("deviceType", TVConstants.DEVICE_TYPE);
            } catch (JSONException e) {
                e.printStackTrace();
            }

            try {
                jsonMetaData.put("time", Calendar.getInstance().getTime().getTime());
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                jsonEvent.put("metaData", jsonMetaData);
            } catch (JSONException e) {
                e.printStackTrace();
            }

            JSONObject payload = new JSONObject();
            for (int i = 0; i < event.names().length(); i++) {
                System.out.println("sinkAA :" + "key = " + event.names().getString(i) + " value = " + event.get(event.names().getString(i)));
                try {
                    payload.put(event.names().getString(i), event.get(event.names().getString(i)));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }

            //payload.put(key, value);
            try {
                jsonEvent.put("payloadData", payload);
            } catch (JSONException e) {
                e.printStackTrace();
            }

            JSONObject wrapper = new JSONObject();
            wrapper.put("event", jsonEvent);
            String topic = topicOption.getValue(dynamicOptions);
            if (androidTVMQTTHandler != null) {
                if (androidTVMQTTHandler.isConnected()) {
                    androidTVMQTTHandler.publishDeviceData(wrapper.toString(), topic);
                } else {
                    Log.i("PublishStats", "Connection not available, hence entry is added to cache");
                    /*cacheManagementService.addCacheEntry(topic, wrapper.toString());
                    isCacheEnabled = true;*/
                }
            }else {
                Log.i("TAG","androidtv mqtt handler not initialized");
            }


        } catch (JSONException e) {
            e.printStackTrace();
        } catch (TransportHandlerException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        if(DeviceManagementService.getAndroidTVMQTTHandler()!=null){
            this.androidTVMQTTHandler = DeviceManagementService.getAndroidTVMQTTHandler();
        }else{
            Log.i("TAG","androidTVMQTTHandler is not initialized");
        }

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }
}
