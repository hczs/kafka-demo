package icu.sunnyc.demo.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 生产者 API 学习
 * 异步发送
 * 异步发送带回调
 * 同步发送
 * @author ：hc
 * @date ：Created in 2022/5/17 21:27
 * @modified ：
 */
public class ProducerSendApi {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // kafka 生产者配置信息对象
        Properties properties = new Properties();
        // bootstrap-server （必须）
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        // key value 序列化器 （必须）传序列化器的全限定类名路径
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 创建 Kafka 生产者对象 在命令行发送的消息其实 key 都是空的
        // 使用 try with resources 方式，不用最后进行 kafkaProducer.close() 操作了
        try( KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties) ) {
            // 异步发送 向 first topic 中发送一个消息
            kafkaProducer.send(new ProducerRecord<>("first", "异步发送消息"));

            // 异步发送 带回调
            kafkaProducer.send(new ProducerRecord<>("first", "异步发送消息带回调函数"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                    }
                }
            });

            // 同步发送 在 send 后加一个 get 就是同步发送了
            kafkaProducer.send(new ProducerRecord<>("first", "同步发送消息")).get();
        }

    }
}
