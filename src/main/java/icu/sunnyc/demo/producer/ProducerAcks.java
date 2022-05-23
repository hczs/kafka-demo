package icu.sunnyc.demo.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 关于应答机制 acks 参数的设置
 * @author ：hc
 * @date ：Created in 2022/5/23 22:52
 * @modified ：
 */
public class ProducerAcks {
    public static void main(String[] args) {
        // kafka 生产者配置信息对象
        Properties properties = new Properties();
        // bootstrap-server （必须）
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        // key value 序列化器 （必须）传序列化器的全限定类名路径
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // （选填）acks 参数设置 默认 -1/all
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        // （选填）重试次数，发送消息失败后的重试次数 默认 Integer.MAX_VALUE
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 发消息测试
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            kafkaProducer.send(new ProducerRecord<>("first", "===houge==="), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("设置acks后的消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                }
            });
        }
    }
}
