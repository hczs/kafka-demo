package icu.sunnyc.demo.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 * @author ：hc
 * @date ：Created in 2022/5/23 21:35
 * @modified ：
 */
public class MyPartitioner implements Partitioner {
    /**
     * 返回消息对应的分区
     * 如果消息中包含 "houge" 那么久发往 1 号分区，否则发往 0 号分区
     * @param topic 主题
     * @param key 消息的key
     * @param keyBytes 序列化后的消息key的字节数组
     * @param value 消息内容
     * @param valueBytes 序列化后的消息的字节数组
     * @param cluster 集群元数据，可以查看分区信息
     * @return 消息对应的分区号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return value.toString().contains("houge") ? 1 : 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
