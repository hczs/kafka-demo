package icu.sunnyc.demo.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka 事务 API
 * @author ：hc
 * @date ：Created in 2022/5/26 22:33
 * @modified ：
 */
public class TransactionDemo {
    public static void main(String[] args) {
        // 生产者配置对象
        Properties properties = new Properties();

        // 给kafka配置对象添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 想用事务，必须设置事务ID
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_0");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            // 初始化事务
            kafkaProducer.initTransactions();
            // 开启事务
            kafkaProducer.beginTransaction();
            try {
                // 发送消息
                kafkaProducer.send(new ProducerRecord<>("first", "事务测试消息5"), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        System.out.println("消息发送成功！" + "主题：" + metadata.topic() + " 分区：" + metadata.partition());
                    }
                });
                int i = 1 / 0;
                // 提交事务
                kafkaProducer.commitTransaction();
                System.out.println("事务成功提交");
            } catch (Exception e) {
                e.printStackTrace();
                // 终止事务
                kafkaProducer.abortTransaction();
                System.out.println("事务终止");
            }
        }
    }
}
