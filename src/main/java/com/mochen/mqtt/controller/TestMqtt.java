package com.mochen.mqtt.controller;

import com.mochen.mqtt.entity.MyMqttClient;
import org.junit.Test;

/**
 * @author 董仁亮
 * @date 2020-11-25 16:04
 * @Description:
 */
public class TestMqtt {

    /**
     * 分级发布与订阅
     *
     * @throws Exception
     */
    String topicName1 = "topic7";

    @Test
    public void testTopic() throws Exception {
        MyMqttClient myMqttClient = new MyMqttClient("tcp://127.0.0.1:61613", "admin", "password", "mqttClient2", 10, 20);
        myMqttClient.connect();
        myMqttClient.subscribe(topicName1);
        while (true);
    }

    @Test
    public void testTopic2() throws Exception {
        MyMqttClient myMqttClient = new MyMqttClient("tcp://127.0.0.1:61613", "admin", "password", "mqttClient4", 10, 20);
        myMqttClient.connect();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            myMqttClient.publish(topicName1 + "发送消息"+i, topicName1, 1, true);
        }

    }


}
