package icu.sunnyc.springboot.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * 消费者消费数据测试
 * @author ：hc
 * @date ：Created in 2022/5/29 16:45
 * @modified ：
 */
@Configuration
public class KafkaConsumer {

    /**
     * 必须配置 groupId 和 topics
     * @param msg 消费到的数据
     */
    @KafkaListener(groupId = "test", topics = "first")
    public void consumeMessage(String msg) {
        System.out.println("消费到数据：" + msg);
    }
}
