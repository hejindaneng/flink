package com.hejin.syn;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * Package: com.hejin.syn
 * Description： TODO
 * Author: Hejin
 * Date: Created in 2019/1/7 22:31
 * Company: 公司
 * Copyright: Copyright (c) 2017
 * Version: 0.0.1
 * Modified By:
 */
public class KafkaProducer {
    String topic;

    public KafkaProducer(String topic) {
        this.topic = topic;
    }

    public static void sendMessage(String topic, String sendKey, String data) {
        Producer producer = createProducer();
        producer.send(new KeyedMessage<String,String>(topic,sendKey,data));
    }

    public static Producer<Integer, String> createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "node01:2181");
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "node01:9092");
        return new Producer<Integer,String>(new ProducerConfig(properties));
    }
}
