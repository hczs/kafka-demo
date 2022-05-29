package icu.sunnyc.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 一个消费者组有多个消费者 消费同一个 topic 测试
 * 正常情况一个分区只能有一个消费者进行消费
 * @author ：hc
 * @date ：Created in 2022/5/29 15:47
 * @modified ：
 */
public class MultiConsumer {

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
        KafkaConsumer<String, String> kafkaConsumer1 = new KafkaConsumer<>(properties);
        KafkaConsumer<String, String> kafkaConsumer2 = new KafkaConsumer<>(properties);
        KafkaConsumer<String, String> kafkaConsumer3 = new KafkaConsumer<>(properties);

        // 订阅 first 主题
        kafkaConsumer1.subscribe(Collections.singletonList("first"));
        kafkaConsumer2.subscribe(Collections.singletonList("first"));
        kafkaConsumer3.subscribe(Collections.singletonList("first"));

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        ArrayList<Callable<Object>> tasks = new ArrayList<>();
        tasks.add(() -> {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer1.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("消费者组 test 中消费者 1 消费到数据：" + record);
                }
            }
        });
        tasks.add(() -> {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer2.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("消费者组 test 中消费者 2 消费到数据：" + record);
                }
            }
        });
        tasks.add(() -> {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer3.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("消费者组 test 中消费者 3 消费到数据：" + record);
                }
            }
        });
        for (Callable<Object> task : tasks) {
            executorService.submit(task);
        }
        System.out.println("任务提交完毕");
    }
}
