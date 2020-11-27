package com.mochen.mqtt.entity;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author 董仁亮
 * @date 2020-11-25 14:59
 * @Description:
 */


public class MyMqttClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyMqttClient.class);
    private static MqttClient client;

    private String host;
    private String username;
    private String password;
    private String clientId;
    private int timeout;
    private int keepalive;


    public static MqttClient getClient() {
        return client;
    }

    public static void setClient(MqttClient client) {
        MyMqttClient.client = client;
    }


    public MyMqttClient(String host, String username, String password, String clientId, int timeout, int keepalive) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.clientId = clientId;
        this.timeout = timeout;
        this.keepalive = keepalive;
    }

    /**
     * 设置连接属性
     *
     * @param username
     * @param password
     * @param timeout
     * @param keepalive
     * @return
     */
    public MqttConnectOptions setMqttConnectOptions(String username, String password, int timeout, int keepalive) {
        MqttConnectOptions options = new MqttConnectOptions();
        //用户名
        options.setUserName(username);
        //密码
        options.setPassword(password.toCharArray());
        // 设置超时时间 单位为秒
        options.setConnectionTimeout(timeout);
        // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送个消息判断客户端是否在线，但这个方法并没有重连的机制
        options.setKeepAliveInterval(keepalive);
        // 设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，设置为true表示每次连接到服务器都以新的身份连接
        options.setCleanSession(false);
        //设置断开后重新连接
        options.setAutomaticReconnect(true);
        return options;
    }

    /**
     * 连接mqtt服务端，得到MqttClient连接对象
     */
    public void connect() throws MqttException {
        if (client == null) {
            //clientid即连接MQTT的客户端ID，一般以唯一标识符表示，MemoryPersistence设置clientid的保存形式，默认为以内存保存
            client = new MqttClient(host, clientId, new MemoryPersistence());
            client.setCallback(new PushCallback(MyMqttClient.this));
        }
        MqttConnectOptions mqttConnectOptions = setMqttConnectOptions(username, password, timeout, keepalive);
        if (!client.isConnected()) {
            client.connect(mqttConnectOptions);
        } else {
            client.disconnect();
            client.connect(mqttConnectOptions);
        }

    }

    /**
     * 发布，默认qos为0，非持久化
     *
     * @param pushMessage
     * @param topic
     */
    public void publish(String pushMessage, String topic) {
        publish(pushMessage, topic, 0, false);
    }

    /**
     * 发布消息
     *
     * @param pushMessage
     * @param topic
     * @param qos
     * @param retained:留存
     */
    public void publish(String pushMessage, String topic, int qos, boolean retained) {
        MqttMessage message = new MqttMessage();
        message.setPayload(pushMessage.getBytes());
        message.setQos(qos);
        //retained为true表示会去取之前还未消费的数据，为false只取最新接收的消息
        message.setRetained(retained);
        MqttTopic mqttTopic = MyMqttClient.getClient().getTopic(topic);
        if (null == mqttTopic) {
            LOGGER.error("topic is not exist");
        }
        MqttDeliveryToken token;//Delivery:配送
        synchronized (this) {//注意：这里一定要同步，否则，在多线程publish的情况下，线程会发生死锁
            try {
                token = mqttTopic.publish(message);//也是发送到执行队列中，等待执行线程执行，将消息发送到消息中间件
                token.waitForCompletion(1000L);
            } catch (MqttPersistenceException e) {
                e.printStackTrace();
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 订阅某个主题，qos默认为0
     *
     * @param topic
     */
    public void subscribe(String topic) {
        subscribe(topic, 0);
    }

    /**
     * 订阅某个主题
     *
     * @param topic
     * @param qos   0：最多一次 、1：最少一次 、2：只有一次
     */
    public void subscribe(String topic, int qos) {
        try {
            MyMqttClient.getClient().subscribe(topic, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }


}
