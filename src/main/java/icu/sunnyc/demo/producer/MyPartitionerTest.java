package icu.sunnyc.demo.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 自定义分区器测试
 * @author ：hc
 * @date ：Created in 2022/5/23 21:43
 * @modified ：
 */
public class MyPartitionerTest {
    public static void main(String[] args) {
        // kafka 生产者配置信息对象
        Properties properties = new Properties();
        // bootstrap-server （必须）
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        // key value 序列化器 （必须）传序列化器的全限定类名路径
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "icu.sunnyc.demo.producer.MyPartitioner");

        // 发消息测试
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            kafkaProducer.send(new ProducerRecord<>("first", "===houge==="), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("带 houge 消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                }
            });

            kafkaProducer.send(new ProducerRecord<>("first", "===OVO==="), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("不带 houge 消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                }
            });
        }
        // 测试结果
        // 带 houge 消息发送成功！主题：first 分区：1
        // 不带 houge 消息发送成功！主题：first 分区：0
    }
}
