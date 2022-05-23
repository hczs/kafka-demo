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

        // （选填）通过合理修改以下生产者参数，来提高 kafka 吞吐量
        // 1.缓冲区大小 默认 32M = 32 * 1024 * 1024L
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024L);
        // 2.批次大小 默认 16K = 16384
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 3.等待时间 默认 0 ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 4.数据压缩 默认 none
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 创建 Kafka 生产者对象 在命令行发送的消息其实 key 都是空的
        // 使用 try with resources 方式，不用最后进行 kafkaProducer.close() 操作了
        try( KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties) ) {
            // 异步发送 向 first topic 中发送一个消息 不指定分区 不指定 key 会使用默认的分区策略 粘性分区
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

            // 指定分区发送
            kafkaProducer.send(new ProducerRecord<>("first", 1, "", "指定1号分区发送"),
                    (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("指定1号分区消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                }
            });

            // 不指定分区，指定 key 分区策略就会把key的hash值取余 partition，来确定要发往哪个分区
            kafkaProducer.send(new ProducerRecord<>("first", "hehe", "指定key发送消息"),
                    (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("指定key消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                }
            });
        }

    }
}
