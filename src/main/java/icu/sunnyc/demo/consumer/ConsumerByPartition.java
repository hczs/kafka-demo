package icu.sunnyc.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * 消费指定分区的数据
 * @author ：hc
 * @date ：Created in 2022/5/29 15:42
 * @modified ：
 */
public class ConsumerByPartition {

    public static void main(String[] args) {
        // 配置信息对象
        Properties properties = new Properties();

        // bootstrap server
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        // key value 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 「必须」设置消费者组 id 不设置不行 命令行不用设置是因为自动生成了消费者组 id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 配置完毕，创建消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 订阅想要消费的主题
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        // sample 只消费 first 主题 0 号分区的数据
        topicPartitions.add(new TopicPartition("first", 0));
        kafkaConsumer.assign(topicPartitions);

        // 拉取数据并打印
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }
}
