package com.mochen.mqtt.entity;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 董仁亮
 * @date 2020-11-25 14:53
 * @Description:
 */
public class PushCallback implements MqttCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushCallback.class);

    private MyMqttClient myMqttClient;

    public PushCallback(MyMqttClient myMqttClient) {
        this.myMqttClient = myMqttClient;
    }


    /**
     * 连接丢失：一般用与重连
     *
     * @param throwable
     */
    @Override
    public void connectionLost(Throwable throwable) {
        long reconnectTimes = 1;

        while (true) {
            try {
                if (MyMqttClient.getClient().isConnected()) {
                    LOGGER.warn("mqtt reconnect success end");
                    return;
                }
                LOGGER.warn("mqtt reconnect times = {} try again...", reconnectTimes++);
                MyMqttClient.getClient().reconnect();
            } catch (MqttException e) {
                LOGGER.error("", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                //e1.printStackTrace();
            }
        }

    }

    /**
     * subscribe后得到的消息会执行到这里面
     *
     * @param topic
     * @param message
     * @throws Exception
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {

        System.out.println("主题:" + topic + ",接收消息内容 : " + new String(message.getPayload()));
    }

    /**
     * (发布）publish后会执行到这里,发送状态
     *
     * @param iMqttDeliveryToken
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
