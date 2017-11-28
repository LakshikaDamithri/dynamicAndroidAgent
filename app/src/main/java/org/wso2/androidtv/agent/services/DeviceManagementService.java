/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.androidtv.agent.services;

import android.app.DownloadManager;
import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.ServiceConnection;
import android.database.Cursor;
import android.net.Uri;
import android.os.Binder;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Message;
import android.support.annotation.Nullable;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.wso2.androidtv.agent.MessageActivity;
import org.wso2.androidtv.agent.R;
import org.wso2.androidtv.agent.VideoActivity;
import org.wso2.androidtv.agent.constants.TVConstants;
import org.wso2.androidtv.agent.h2cache.H2Connection;
import org.wso2.androidtv.agent.mqtt.AndroidTVMQTTHandler;
import org.wso2.androidtv.agent.mqtt.MessageReceivedCallback;
import org.wso2.androidtv.agent.cache.CacheEntry;
import org.wso2.androidtv.agent.mqtt.transport.TransportHandlerException;
import org.wso2.androidtv.agent.subscribers.EdgeSourceSubscriber;
import org.wso2.androidtv.agent.util.AndroidTVUtils;
import org.wso2.androidtv.agent.util.LocalRegistry;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
//import org.wso2.extension.siddhi.map.text.sourcemapper.TextSourceMapper;
//import org.wso2.siddhi.extension.input.mapper.text.TextSourceMapper;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class DeviceManagementService extends Service {

    private static final String TAG = UsbService.class.getSimpleName();

    private static AndroidTVMQTTHandler androidTVMQTTHandler;
    private UsbService usbService;
    private SiddhiService siddhiService;
    private UsbServiceHandler usbServiceHandler;
    private SiddhiServiceHandler siddhiServiceHandler;
    private boolean hasPendingConfigDownload = false;
    private long downloadId = -1;
    private CacheManagementService cacheManagementService;

    private static volatile boolean waitFlag = false;
    private static volatile boolean isSyncPaused = false;
    private static volatile boolean isSyncStopped = false;
    private static volatile boolean isInCriticalPath = false;
    private static volatile String serialOfCurrentEdgeDevice = "";
    private static volatile String incomingMessage = "";
    private static volatile String sendingMessage = "";
    private static volatile boolean isCacheEnabled = false;
    //private static volatile SourceEventListener sourceEventListener;
   // private static ArrayList<Source> sources = new ArrayList<>();
    private static ArrayList<EdgeSourceSubscriber> sourceSubscribers = new ArrayList<>();

    private DownloadManager downloadManager;


    private final ServiceConnection usbConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName componentName, IBinder iBinder) {
            usbService = ((UsbService.UsbBinder) iBinder).getService();
            usbService.setHandler(usbServiceHandler);
            isInCriticalPath = false;
            isSyncStopped = false;
            new Thread(syncScheduler).start();
        }

        @Override
        public void onServiceDisconnected(ComponentName arg0) {
            isSyncStopped = true;
            usbService = null;
        }
    };

    private final ServiceConnection siddhiConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName componentName, IBinder iBinder) {
            siddhiService = ((SiddhiService.SiddhiBinder) iBinder).getService();
            siddhiService.setHandler(siddhiServiceHandler);
        }

        @Override
        public void onServiceDisconnected(ComponentName arg0) {
            siddhiService = null;
        }
    };

    private Runnable syncScheduler = new Runnable() {
        @Override
        public void run() {
            while (!isSyncStopped) {
                try {
                    for (String serial : LocalRegistry.getEdgeDevices(getApplicationContext())) {
                        while ((isInCriticalPath || isSyncPaused) && !isSyncStopped) {
                            Thread.sleep(1000);
                        }
                        if (isSyncStopped) {
                            break;
                        }
                        isInCriticalPath = true;
                        Thread.sleep(1000);
                        serialOfCurrentEdgeDevice = serial;
                        String serialH = serial.substring(0, serial.length() - 8);
                        String serialL = serial.substring(serial.length() - 8);
                        sendConfigLine("+++");
                        sendConfigLine("ATDH" + serialH + "\r");
                        sendConfigLine("ATDL" + serialL + "\r");
                        sendConfigLine("ATCN\r");
                        Thread.sleep(1000);
                        usbService.write("D\r".getBytes());
                        Thread.sleep(5000);
                        isInCriticalPath = false;
                        Thread.sleep(1000);
                        if (isSyncStopped) {
                            break;
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    Log.e(TAG, e.getMessage(), e);
                }
            }
        }
    };

    private BroadcastReceiver configDownloadReceiver = new BroadcastReceiver() {
        public void onReceive(Context context, Intent intent) {
            Bundle extras = intent.getExtras();
            long currentId = extras.getLong(DownloadManager.EXTRA_DOWNLOAD_ID);
            if (downloadId == currentId) {
                DownloadManager.Query q = new DownloadManager.Query();
                q.setFilterById(currentId);
                Cursor c = downloadManager.query(q);
                if (c.moveToFirst()) {
                    int status = c.getInt(c.getColumnIndex(DownloadManager.COLUMN_STATUS));
                    if (status == DownloadManager.STATUS_SUCCESSFUL) {
                        unregisterReceiver(configDownloadReceiver);
                        hasPendingConfigDownload = false;
                        String downloadFileLocalUri = c.getString(c.getColumnIndex(DownloadManager.COLUMN_LOCAL_URI));
                        if (downloadFileLocalUri != null) {
                            File mFile = new File(Uri.parse(downloadFileLocalUri).getPath());
                            String downloadFilePath = mFile.getAbsolutePath();
                            installConfigurations(downloadFilePath);
                        }
                    }
                }
                c.close();
            }
        }
    };


    private void installConfigurations(final String fileName) {
        Runnable installConfigThread = new Runnable() {
            @Override
            public void run() {
                if (usbService != null) {
                    isSyncPaused = true;
                    while (isInCriticalPath) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    isInCriticalPath = true;
                    try {
                        sendConfigLine("+++");
                        File fXmlFile = new File(fileName);
                        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                        Document doc = dBuilder.parse(fXmlFile);
                        doc.getDocumentElement().normalize();

                        NodeList nList = doc.getElementsByTagName("setting");
                        for (int i = 0; i < nList.getLength(); i++) {
                            Node nNode = nList.item(i);
                            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                                Element eElement = (Element) nNode;
                                String atCommand = "AT" + eElement.getAttribute("command") + eElement.getTextContent() + "\r";
                                sendConfigLine(atCommand);
                            }
                        }
                        sendConfigLine("ATWR\r");
                        sendConfigLine("ATAC\r");
                    } catch (Exception e) {
                        usbService.write("ATNR0\r".getBytes());
                        Log.e(TAG, e.getMessage(), e);
                    }
                    Log.i(TAG, "Configs updated");
                    waitFlag = true;
                    usbService.write("ATCN\r".getBytes());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                    isInCriticalPath = false;
                    isSyncPaused = true;
                }
            }
        };
        new Thread(installConfigThread).start();
    }

    private void sendConfigLine(String line) throws InterruptedException {
        waitFlag = true;
        int count = 0;
        usbService.write(line.getBytes());
        while (waitFlag) {
            Thread.sleep(100);
            if (count++ > 10) {
                usbService.write("+++".getBytes());
                break;
            }
        }
    }


    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return Service.START_STICKY;
    }

    @Override
    public void onCreate() {




       androidTVMQTTHandler = new AndroidTVMQTTHandler(this, new MessageReceivedCallback() {
            @Override
            public void onMessageReceived(JSONObject message) throws JSONException {
                performAction(message.getString("action"), message.getString("payload"));
            }
        });
       androidTVMQTTHandler.connect();

       H2Connection h2Connection = new H2Connection(this);
        try {
            h2Connection.initializeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        usbServiceHandler = new UsbServiceHandler(this);
        // Start UsbService(if it was not started before) and Bind it
        startService(UsbService.class, usbConnection, null);

        siddhiServiceHandler = new SiddhiServiceHandler(this);
        Bundle extras = new Bundle();
        /*String executionPlan = "@app:name('edgeAnalytics') " +
                "@source(type='textEdge', @map(type='text', fail.on.missing.attribute = 'true',regex.T='\"t\"\\:(\\w+)',regex.H=’\"h\"\\:(\\w+)’,regex.A=’,\"a\"\\:(\\w+)’,regex.W=’\"w\"\\:(\\w+)’,regex.K=’\"k\"\\:(\\w+)’, regex.L=’\"l\"\\:(\\w+)’,@attributes(temperature = \"T\", humidity = \"H\", ac = \"A\", window = \"W\", keycard = \"K\",light = \"L\")))" +
                "define stream edgeDeviceEventStream " +
                "(ac int, window int, light int, temperature float, humidity float, keycard int); " +
                "@info(name = 'alertQuery') " +
                "from edgeDeviceEventStream[(1 == ac or 1 == window or 1 == light) and 0 == keycard] " +
                "select ac, window, light insert into alertOutputStream; " +
                "@info(name = 'temperatureQuery') " +
                "from every te1=edgeDeviceEventStream, te2=edgeDeviceEventStream[te1.temperature != temperature ] " +
                "select te2.temperature insert into temperatureOutputStream; " +
                "@info(name = 'humidityQuery') " +
                "from every he1=edgeDeviceEventStream, he2=edgeDeviceEventStream[he1.humidity != humidity ] " +
                "select he2.humidity insert into humidityOutputStream; " +
                "@info(name = 'acQuery') " +
                "from every ae1=edgeDeviceEventStream, ae2=edgeDeviceEventStream[ae1.ac != ac ] " +
                "select ae2.ac insert into acOutputStream; " +
                "@info(name = 'windowQuery') " +
                "from every we1=edgeDeviceEventStream, we2=edgeDeviceEventStream[we1.window != window ] " +
                "select we2.window insert into windowOutputStream; " +
                "@info(name = 'keycardQuery') " +
                "from every ke1=edgeDeviceEventStream, ke2=edgeDeviceEventStream[ke1.keycard != keycard ] " +
                "select ke2.keycard insert into keycardOutputStream;";*/

       /* String executionPlan = "@app:name('edgeAnalytics') " +
                "@source(type='textEdge', @map(type='text', fail.on.missing.attribute = 'true' , regex" +
                ".T=\"\"\"\"t\":(\\w+)\"\"\", regex"+".H=\"\"\"\"h\":(\\w+)\"\"\", regex"+".A=\"\"\"\"a\":(\\w+)\"\"\", regex"+".W=\"\"\"\"w\":(\\w+)\"\"\", regex"+".K=\"\"\"\"k\":(\\w+)\"\"\", regex"+".L=\"\"\"\"l\":(\\w+)\"\"\", @attributes(temperature = 'T', humidity = 'H', ac = 'A', window = 'W', keycard = 'K', light = 'L')))"+
                "define stream edgeDeviceEventStream " +
                "(ac int, window int, light int, temperature float, humidity float, keycard int); " +
                "@info(name = 'alertQuery') " +
                "from edgeDeviceEventStream[(1 == ac or 1 == window or 1 == light) and 0 == keycard] " +
                "select ac, window, light insert into alertOutputStream; " +
                "@info(name = 'temperatureQuery') " +
                "from every te1=edgeDeviceEventStream, te2=edgeDeviceEventStream[te1.temperature != temperature ] " +
                "select te2.temperature insert into temperatureOutputStream; " +
                "@info(name = 'humidityQuery') " +
                "from every he1=edgeDeviceEventStream, he2=edgeDeviceEventStream[he1.humidity != humidity ] " +
                "select he2.humidity insert into humidityOutputStream; " +
                "@info(name = 'acQuery') " +
                "from every ae1=edgeDeviceEventStream, ae2=edgeDeviceEventStream[ae1.ac != ac ] " +
                "select ae2.ac insert into acOutputStream; " +
                "@info(name = 'windowQuery') " +
                "from every we1=edgeDeviceEventStream, we2=edgeDeviceEventStream[we1.window != window ] " +
                "select we2.window insert into windowOutputStream; " +
                "@info(name = 'keycardQuery') " +
                "from every ke1=edgeDeviceEventStream, ke2=edgeDeviceEventStream[ke1.keycard != keycard ] " +
                "select ke2.keycard insert into keycardOutputStream;";*/
        System.out.println("brokerUrl "+LocalRegistry.getMqttEndpointSiddhi());
        System.out.println("usernameToken "+LocalRegistry.getAccessTokenSidhhi());
        WeakReference<DeviceManagementService> mService;

        String executionPlan = "@app:name('edgeAnalytics') " +
                "@source(type='textEdge', @map(type='text', fail.on.missing.attribute = 'true' , regex" +
                ".T=\"\"\"\"t\":(\\w+)\"\"\", regex"+".H=\"\"\"\"h\":(\\w+)\"\"\", regex"+".A=\"\"\"\"a\":(\\w+)\"\"\", regex"+".W=\"\"\"\"w\":(\\w+)\"\"\", regex"+".K=\"\"\"\"k\":(\\w+)\"\"\", regex"+".L=\"\"\"\"l\":(\\w+)\"\"\", @attributes(temperature = 'T', humidity = 'H', ac = 'A', window = 'W', keycard = 'K', light = 'L')))"+
                "define stream edgeDeviceEventStream " +
                "(ac Float, window float, light float, temperature float, humidity float, keycard float); " +

                "@source(type='textEdge', @map(type='text', fail.on.missing.attribute = 'true', regex"+ ".L=(LON), @attributes(light = 'L') ))" +
                "define stream EventStream1 " +
                "(light string);"+

                /*"@sink(type='mqtt', url="+ "'"+LocalRegistry.getMqttEndpointSiddhi()+"'"+"," +
                "topic="+"'carbon.super/androidtv234/00000000-1209-8a12-0033-c5870033c587/AC',username="+"'"+LocalRegistry.getAccessTokenSidhhi()+"'"+"," +
                "password="+"\""+"\""+", clean.session='true', message.retain='true', " +
                "quality.of.service= '2', keep.alive= '3600',connection.timeout = '5000'," +
                "@map(type='json'))" +
                " define stream acOutputStreamTwo (ac float);" +*/

                /*"@sink(type='mqtt', url="+ "'"+LocalRegistry.getMqttEndpointSiddhi()+"'"+"," +
                "topic="+"'carbon.super/androidtv234/00000000-1209-8a12-0033-c5870033c587/AC',username="+"'"+LocalRegistry.getAccessTokenSidhhi()+"'"+"," +
                "password='', clean.session='true', message.retain='true', " +
                "quality.of.service= '2', keep.alive= '3600',connection.timeout = '5000'," +
                "@map(type='json'))" +
                " define stream acOutputStreamTwo (ac float);" +*/


               /* "@sink(type='mqtt', url="+ "'"+LocalRegistry.getMqttEndpointSiddhi()+"'"+"," +
                "topic="+"'carbon.super/androidtv234/00000000-1209-8a12-0033-c5870033c587/AC',username=55ea0647-fb89-3f52-8a70-56c6a56753e9"+"," +
                "password="+"\""+"\""+", clean.session='true', message.retain='true', " +
                "quality.of.service= '2', keep.alive= '3600',connection.timeout = '5000'," +
                "@map(type='json'))" +
                " define stream acOutputStreamTwo (ac float);" +*/


                /*"@sink(type='mqtt', url="+ "'"+LocalRegistry.getMqttEndpointSiddhi()+"'"+"," +
                "topic="+"'carbon.super/androidtv/00000000-1209-8a12-0033-c5870033c587/TEMP',username="+"'"+LocalRegistry.getAccessTokenSidhhi()+"'"+"," +
                "clean.session='false', message.retain='false', " +
                "quality.of.service= '2', keep.alive= '3600'," +
                "@map(type='json'))"+
                " define stream temperatureOutputStream (temperature float);" +*/
                //"@config(async = 'true') define stream acOutputStream (name String, value float);"+
                "@sink(type='edgeGateway',topic='carbon.super/androidtv/00000000-1209-8a12-0033-c5870033c587/AC',@map(type='json'))"+"define stream acOutputStream (AC Float);"+
                "@sink(type='edgeGateway',topic='carbon.super/androidtv/00000000-1209-8a12-0033-c5870033c587/HUMIDITY',@map(type='json'))"+"define stream humidityOutputStream (HUMIDITY Float);"+
                "@sink(type='edgeGateway',topic='carbon.super/androidtv/00000000-1209-8a12-0033-c5870033c587/TEMP',@map(type='json'))"+"define stream temperatureOutputStream (TEMP Float);"+
                "@sink(type='edgeGateway',topic='carbon.super/androidtv/00000000-1209-8a12-0033-c5870033c587/WINDOW',@map(type='json'))"+"define stream windowOutputStream (WINDOW Float);"+

                "@config(async = 'true') define stream alertStream (alertMessage String);"+


                "from every ae1=edgeDeviceEventStream, ae2=edgeDeviceEventStream[ae1.ac != ac ] " +
                "select ae2.ac as AC insert into acOutputStream; "+

                "from every he1=edgeDeviceEventStream, he2=edgeDeviceEventStream[he1.humidity != humidity ] " +
                "select he2.humidity as HUMIDITY insert into humidityOutputStream; "+

                "from every te1=edgeDeviceEventStream, te2=edgeDeviceEventStream[te1.temperature != temperature ] " +
                "select te2.temperature as TEMP insert into temperatureOutputStream;"+

                "from every we1=edgeDeviceEventStream, we2=edgeDeviceEventStream[we1.window != window ] " +
                "select we2.window as WINDOW insert into windowOutputStream; "+

                "from every aet1=edgeDeviceEventStream, aet2=edgeDeviceEventStream[aet1.ac != ac ] " +
                "select aet2.ac as AC insert into actable; "+

                "from edgeDeviceEventStream[(1 == ac and 0 == window and 0 == light) and 0 == keycard] " +
                "select 'AC is on' as alertMessage insert into alertStream; ";


                /*"@info(name = 'acQuery') " +

                "from every te1=edgeDeviceEventStream, te2=edgeDeviceEventStream[te1.temperature != temperature ] " +
                "select 'tempStream' as name, te2.temperature as value insert into temperatureOutputStream; "+
                "from every he1=edgeDeviceEventStream, he2=edgeDeviceEventStream[he1.humidity != humidity ] " +
                "select 'humidityStream' as name, he2.humidity as value insert into humidityOutputStream; " +
                "from every ae1=edgeDeviceEventStream, ae2=edgeDeviceEventStream[ae1.ac != ac ] " +
                "select 'acStream' as name, ae2.ac as value insert into acOutputStream; "+
                "from every we1=edgeDeviceEventStream, we2=edgeDeviceEventStream[we1.window != window ] " +
                "select 'windowStream' as name, we2.window as value insert into windowOutputStream; "+
                "from every ke1=edgeDeviceEventStream, ke2=edgeDeviceEventStream[ke1.keycard != keycard ] " +
                "select 'keyCardStream' as name, ke2.keycard as value insert into keycardOutputStream;"+*/



                /*"@info(name = 'alertQuery') " +
                "from edgeDeviceEventStream[(1 == ac or 1 == window or 1 == light) and 0 == keycard] " +
                "select ac, window, light insert into alertOutputStream; ";*/









                /*  "@info(name = 'temperatureQuery') " +
                "from every te1=edgeDeviceEventStream, te2=edgeDeviceEventStream[te1.temperature != temperature ] " +
                "select te2.temperature insert into temperatureOutputStream; "+*/
                /* +
                "@info(name = 'temperatureQuery') " +
                "from every te1=edgeDeviceEventStream, te2=edgeDeviceEventStream[te1.temperature != temperature ] " +
                "select te2.temperature insert into temperatureOutputStream; " +
                "@info(name = 'humidityQuery') " +
                "from every he1=edgeDeviceEventStream, he2=edgeDeviceEventStream[he1.humidity != humidity ] " +
                "select he2.humidity insert into humidityOutputStream; " +
                "@info(name = 'windowQuery') " +
                "from every we1=edgeDeviceEventStream, we2=edgeDeviceEventStream[we1.window != window ] " +
                "select we2.window insert into windowOutputStream; " +
                "@info(name = 'keycardQuery') " +
                "from every ke1=edgeDeviceEventStream, ke2=edgeDeviceEventStream[ke1.keycard != keycard ] " +
                "select ke2.keycard insert into keycardOutputStream;";*/

        /*String executionPlan = "@app:name('edgeAnalytics') " +
                "@source(type='textEdge', @map(type='text', fail.on.missing.attribute = 'true' , regex" +
                ".T=\"\"\"\"t\":(\\w+)\"\"\", regex"+".H=\"\"\"\"h\":(\\w+)\"\"\", regex"+".A=\"\"\"\"a\":(\\w+)\"\"\", regex"+".W=\"\"\"\"w\":(\\w+)\"\"\", regex"+".K=\"\"\"\"k\":(\\w+)\"\"\", regex"+".L=\"\"\"\"l\":(\\w+)\"\"\", @attributes(temperature = 'T', humidity = 'H', ac = 'A', window = 'W', keycard = 'K', light = 'L')))"+
                "define stream edgeDeviceEventStream " +
                "(ac int, window int, light int, temperature float, humidity float, keycard int); " +


                "@sink(type='mqtt', url="+ "'"+LocalRegistry.getMqttEndpointSiddhi()+"'"+"," +
                "topic="+"'carbon.super/androidtv/00000000-1209-8a12-0033-c5870033c587/AC',username="+"'admin'"+"," +"password='admin',"+
                "clean.session='true', message.retain='false', " +
                "quality.of.service= '0', keep.alive= '600000'," +
                "@map(type='json'))" +
                " define stream acOutputStream (ac int);" +
                "@info(name = 'acQuery') " +
                "from every ae1=edgeDeviceEventStream, ae2=edgeDeviceEventStream[ae1.ac != ac ] " +
                "select ae2.ac insert into acOutputStream; "+
                "@info(name = 'alertQuery') " +
                "from edgeDeviceEventStream[(1 == ac or 1 == window or 1 == light) and 0 == keycard] " +
                "select ac, window, light insert into alertOutputStream; " +
                "@info(name = 'temperatureQuery') " +
                "from every te1=edgeDeviceEventStream, te2=edgeDeviceEventStream[te1.temperature != temperature ] " +
                "select te2.temperature insert into temperatureOutputStream; " +
                "@info(name = 'humidityQuery') " +
                "from every he1=edgeDeviceEventStream, he2=edgeDeviceEventStream[he1.humidity != humidity ] " +
                "select he2.humidity insert into humidityOutputStream; " +
                "@info(name = 'windowQuery') " +
                "from every we1=edgeDeviceEventStream, we2=edgeDeviceEventStream[we1.window != window ] " +
                "select we2.window insert into windowOutputStream; " +
                "@info(name = 'keycardQuery') " +
                "from every ke1=edgeDeviceEventStream, ke2=edgeDeviceEventStream[ke1.keycard != keycard ] " +
                "select ke2.keycard insert into keycardOutputStream;";*/
        extras.putString(TVConstants.EXECUTION_PLAN_EXTRA, executionPlan);
        startService(SiddhiService.class, siddhiConnection, extras);

        downloadManager = (DownloadManager) getSystemService(Context.DOWNLOAD_SERVICE);
        //runCacheManagementService();
    }

    @Override
    public void onDestroy() {
        unbindService(usbConnection);
        unbindService(siddhiConnection);
        if (androidTVMQTTHandler != null && androidTVMQTTHandler.isConnected()) {
            androidTVMQTTHandler.disconnect();
        }
        androidTVMQTTHandler = null;
        if (hasPendingConfigDownload) {
            unregisterReceiver(configDownloadReceiver);
            hasPendingConfigDownload = false;
        }
        if (cacheManagementService != null) {
            cacheManagementService.removeAllCacheEntries();
        }
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private void performAction(String action, String payload) {
        switch (action) {
            case "video":
                startActivity(VideoActivity.class, payload);
                break;
            case "message":
                startActivity(MessageActivity.class, payload);
                break;
            case "config-url":
                configureXBee(payload);
                break;
            case "xbee-add":
                LocalRegistry.addEdgeDevice(getApplicationContext(), payload);
                serialOfCurrentEdgeDevice = payload;
                break;
            case "xbee-remove":
                LocalRegistry.removeEdgeDevice(getApplicationContext(), payload);
                break;
            case "xbee-command":
                sendCommandToEdgeDevice(payload);
                break;
            case "edgeQuery":
                Bundle extras = new Bundle();
                extras.putString(TVConstants.EXECUTION_PLAN_EXTRA, payload);
                startService(SiddhiService.class, siddhiConnection, extras);
                break;
        }
    }

    private void sendCommandToEdgeDevice(final String payload) {
        if (usbService != null) {
            Runnable sendCommandThread = new Runnable() {
                @Override
                public void run() {
                    try {
                        isSyncPaused = true;
                        while (isInCriticalPath) {
                            Thread.sleep(1000);
                        }
                        isInCriticalPath = true;
                        Thread.sleep(1000);
                        JSONObject commandJSON = new JSONObject(payload);
                        String serial = commandJSON.getString("serial");
                        String command = commandJSON.getString("command") + "\r";
                        String serialH = serial.substring(0, serial.length() - 8);
                        String serialL = serial.substring(serial.length() - 8);
                        sendConfigLine("+++");
                        sendConfigLine("ATDH" + serialH + "\r");
                        sendConfigLine("ATDL" + serialL + "\r");
                        sendConfigLine("ATCN\r");
                        Thread.sleep(1000);
                        usbService.write(command.getBytes());
                        Thread.sleep(5000);
                    } catch (JSONException | InterruptedException e) {
                        Log.e(TAG, e.getClass().getSimpleName(), e);
                    } finally {
                        isInCriticalPath = false;
                        isSyncPaused = false;
                    }
                }
            };
            new Thread(sendCommandThread).start();
        }
    }

    private void startActivity(Class<?> cls, String extra) {
        Intent intent = new Intent(this, cls);
        intent.putExtra(TVConstants.MESSAGE, extra);
        intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        startActivity(intent);
    }

    private synchronized void receivedXBeeData(String message) {
        incomingMessage += message;
        if (incomingMessage.endsWith("\r")) {
            message = incomingMessage;
            incomingMessage = "";
            processXBeeMessage(message.replace("\r", ""));

        }
    }

    private void processXBeeMessage(String message) {
        if (waitFlag && "OK".equals(message)) {
            waitFlag = false;
        } else {
            Log.i(TAG, "Message> " + message);
            try {
                JSONObject incomingMsg = new JSONObject(message);
                switch (incomingMsg.getString("a")) {
                    case "LON":
                        sendATResponse("Light ON");
                        break;
                    case "LOFF":
                        sendATResponse("Light OFF");
                        break;
                    case "DOPEN":
                        sendATResponse("Door lock opened");
                        break;
                    case "DCLOSE":
                        sendATResponse("Door lock closed");
                        break;
                    case "CIN":
                        sendATResponse("Access card inserted");
                        break;
                    case "CO":
                        sendATResponse("Access card removed");
                        break;
                    case "DATA":

                        /*JSONObject payload = incomingMsg.getJSONObject("p");
                        float temp = payload.getInt("t");
                        float humidity = payload.getInt("h");
                        int ac = payload.getInt("a");
                        int window = payload.getInt("w");
                        int light = payload.getInt("l");
                        int keyCard = payload.getInt("k");*/
                        //siddhiService.getInputHandler().send(new Object[]{ac, window, light, temp, humidity, keyCard});
                        for(EdgeSourceSubscriber sourceSubscriber:sourceSubscribers) {
                            //sourceEventListener.onEvent(message, null);
                            sourceSubscriber.recieveEvent(message,null);
                        }
                        break;
                }
            } catch (JSONException e) {
                Log.e(TAG, "Incomplete incoming message. Ignored", e);
            }/*catch (InterruptedException e) {
                Log.e(TAG, e.getClass().getSimpleName(), e);
            }*/
        }
    }

    private void sendATResponse(String message) {
        try {
            JSONObject jsonEvent = new JSONObject();
            JSONObject jsonMetaData = new JSONObject();
            jsonMetaData.put("owner", LocalRegistry.getUsername(getApplicationContext()));
            jsonMetaData.put("deviceId", getDeviceId());
            jsonMetaData.put("deviceType", TVConstants.DEVICE_TYPE);
            jsonMetaData.put("time", Calendar.getInstance().getTime().getTime());
            jsonEvent.put("metaData", jsonMetaData);

            JSONObject payload = new JSONObject();
            payload.put("serial", serialOfCurrentEdgeDevice);
            payload.put("at_response", message);
            jsonEvent.put("payloadData", payload);

            JSONObject wrapper = new JSONObject();
            wrapper.put("event", jsonEvent);
            if (androidTVMQTTHandler.isConnected()) {
                androidTVMQTTHandler.publishDeviceData(wrapper.toString());
            } else {
                Log.i("SendATResponse", "Connection not available, hence entry is added to cache");
                cacheManagementService.addCacheEntry(androidTVMQTTHandler.getDefaultPublishTopic(),
                        wrapper.toString());
                isCacheEnabled = true;
            }
        } catch (TransportHandlerException | JSONException e) {
            Log.e(TAG, e.getClass().getSimpleName(), e);
        }
    }

    private void publishStats(String topic,String streamNameKey, String streamNameValue, String key, float value) {
        try {
            JSONObject jsonEvent = new JSONObject();
            JSONObject jsonMetaData = new JSONObject();
            jsonMetaData.put("owner", LocalRegistry.getUsername(getApplicationContext()));
            jsonMetaData.put("deviceId", getDeviceId());
            jsonMetaData.put("deviceType", TVConstants.DEVICE_TYPE);
            jsonMetaData.put("time", Calendar.getInstance().getTime().getTime());
            jsonEvent.put("metaData", jsonMetaData);

            JSONObject payload = new JSONObject();
            payload.put(streamNameKey,streamNameValue);
            payload.put(key, value);
            jsonEvent.put("payloadData", payload);

            JSONObject wrapper = new JSONObject();
            wrapper.put("event", jsonEvent);
            if (androidTVMQTTHandler.isConnected()) {
                androidTVMQTTHandler.publishDeviceData(wrapper.toString(), topic);
            } else {
                Log.i("PublishStats", "Connection not available, hence entry is added to cache");
                cacheManagementService.addCacheEntry(topic, wrapper.toString());
                isCacheEnabled = true;
            }
        } catch (TransportHandlerException | JSONException e) {
            Log.e(TAG, e.getClass().getSimpleName(), e);
        }
    }

    public void displayAlert(boolean ac, boolean window, boolean light) {
        String alertMsg = "Please ";
        if (window) {
            alertMsg += "close the window ";
        }
        if (ac || light) {
            if (window) {
                alertMsg += "and ";
            }
            alertMsg += "turn off ";
            if (ac) {
                alertMsg += "AC ";
            }
            if (ac && light) {
                alertMsg += "and ";
            }
            if (light) {
                alertMsg += "Light ";
            }
        }
        alertMsg += "before leaving the room.";
        startActivity(MessageActivity.class, alertMsg);
    }

    private String getDeviceId() {
        return AndroidTVUtils.generateDeviceId(getBaseContext(), getContentResolver());
    }

    private void startService(Class<?> service, ServiceConnection serviceConnection, Bundle extras) {
        Intent startServiceIntent = new Intent(this, service);

        if (extras != null && !extras.isEmpty()) {
            Set<String> keys = extras.keySet();
            for (String key : keys) {
                String extra = extras.getString(key);
                startServiceIntent.putExtra(key, extra);
            }
        }
        startService(startServiceIntent);
        Intent bindingIntent = new Intent(this, service);
        bindService(bindingIntent, serviceConnection, Context.BIND_AUTO_CREATE);
    }

    private void configureXBee(String configUrl) {
        try {
            configUrl = URLDecoder.decode(configUrl, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            Log.e(TAG, e.getMessage(), e);
        }
        DownloadManager.Request request = new DownloadManager.Request(Uri.parse(configUrl));
        request.setTitle(getResources().getString(R.string.app_name));
        request.setDescription("Downloading XBee configurations");
        request.setDestinationUri(null);
        request.setNotificationVisibility(DownloadManager.Request.VISIBILITY_HIDDEN);

        // get download service and enqueue file
        downloadId = downloadManager.enqueue(request);
        registerReceiver(configDownloadReceiver, new IntentFilter(DownloadManager.ACTION_DOWNLOAD_COMPLETE));
    }

    // This handler will be passed to UsbService. Data received from serial port is displayed through this handler
    private static class UsbServiceHandler extends Handler {
        private final WeakReference<DeviceManagementService> mService;

        UsbServiceHandler(DeviceManagementService service) {
            mService = new WeakReference<>(service);
        }

        @Override
        public void handleMessage(Message msg) {
            switch (msg.what) {
                case UsbService.MESSAGE_FROM_SERIAL_PORT:
                    String data = (String) msg.obj;
                    mService.get().receivedXBeeData(data);
                    break;
            }
        }
    }

    // This handler will be passed to SiddhiService. Data received from SiddhiQuery is displayed through this handler
    private static class SiddhiServiceHandler extends Handler {
        private final WeakReference<DeviceManagementService> mService;
        private static String publishTopic;

        SiddhiServiceHandler(DeviceManagementService service) {
            mService = new WeakReference<>(service);
            publishTopic = LocalRegistry.getTenantDomain(mService.get()) + "/" + TVConstants.DEVICE_TYPE + "/" +
                    LocalRegistry.getDeviceId(mService.get());
        }

        @Override
        public void handleMessage(Message msg) {
            Event data = (Event) msg.obj;
            Log.d(TAG, "Edge data received: " + data.toString());
            switch (msg.what) {
                case SiddhiService.MESSAGE_FROM_SIDDHI_SERVICE_ALERT_QUERY:
                    //mService.get().publishStats(publishTopic + "/ALERT", "operation", new Float ("1"));
                    mService.get().displayAlert((Float) data.getData(0) == 1, (Float) data.getData(1) == 1, (Float) data.getData(2) == 1);
                    break;
            }
        }
    }

    private void runCacheManagementService() {
        cacheManagementService = new CacheManagementService(getApplicationContext());
        final long threadWaitingTime = 10000; //10 seconds
        if (cacheManagementService.getNumberOfEntries() > 0) {
            isCacheEnabled = true;
        }

        Log.d("CacheManagementService", "Background process is started");
        Thread cacheThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Log.d("CacheManagementService", "Searching for a connection");
                    if (isCacheEnabled) {
                        Log.d("CacheManagementService", "Number of cache entries: "
                                + cacheManagementService.getNumberOfEntries());
                        if (androidTVMQTTHandler.isConnected()) {
                            Log.d("CacheManagementService", "Connection is established");
                            try {
                                publishCacheData();
                            } catch (TransportHandlerException e) {
                                Log.e("CacheManagementService", "Unable to publish cached data", e);
                            }
                        } else {
                            Log.d("CacheManagerService", "Unable to connect to the MQTT server, " +
                                    "hence retry in " + (threadWaitingTime) / 1000 + " seconds");
                        }
                    }
                    try {
                        Thread.sleep(threadWaitingTime);
                    } catch (InterruptedException e) {
                        Log.e("CacheManagementService", "Error occurred while checking cache", e);
                    }
                }
            }
        });
        cacheThread.start();
    }

    private void publishCacheData() throws TransportHandlerException {
        List<CacheEntry> cacheEntries = cacheManagementService.getCacheEntries();
        Log.d("PublishCacheData", "Publishing cached data to the server");
        for (CacheEntry entry : cacheEntries) {
            if (androidTVMQTTHandler.isConnected()) {
                Log.d("PublishCacheData", "Publishing cache entry: " + entry.getId());
                androidTVMQTTHandler.publishDeviceData(entry.getMessage(), entry.getTopic());
                cacheManagementService.removeCacheEntry(entry.getId());
            } else {
                break;
            }
        }
        cacheEntries = cacheManagementService.getCacheEntries();
        if (cacheEntries.size() == 0) {
            Log.d("PublishCacheData", "Cache disabled");
            isCacheEnabled = false;
        }
    }


    public static void connectToSource(EdgeSourceSubscriber sourceSubscriber) {
        sourceSubscribers.add(sourceSubscriber);
        //sourceEventListener = srcEvntListner;
    }

    public static void disConnectToSource(EdgeSourceSubscriber sourceSubscriber) {
        sourceSubscribers.remove(sourceSubscriber);
    }

    public static AndroidTVMQTTHandler getAndroidTVMQTTHandler(){
        return androidTVMQTTHandler;
    }


}
