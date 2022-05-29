package icu.sunnyc.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 用于测试 Consumer 的各种消费数据
 * @author ：hc
 * @date ：Created in 2022/5/29 16:03
 * @modified ：
 */
public class SendForConsumer {

    public static void main(String[] args) {
        // kafka 生产者配置信息对象
        Properties properties = new Properties();
        // bootstrap-server （必须）
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        // key value 序列化器 （必须）传序列化器的全限定类名路径
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try( KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties) ) {
            // 均匀的向三个分区发送数据 0 1 2 三个分区
            for (int i = 0; i < 9; i++) {
                int partition = i % 3;
                // 指定分区发送
                kafkaProducer.send(new ProducerRecord<>("first", partition, "", "指定" + partition + "号分区发送"),
                        (metadata, exception) -> {
                            if (exception == null) {
                                System.out.println("指定1号分区消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                            }
                        });
            }
        }
    }
}
