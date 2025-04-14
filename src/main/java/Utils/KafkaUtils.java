package Utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaUtils {
    public static  String kafkaServer="10.1.3.37:9092";

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupID){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("fetch.min.bytes", "10485760"); // 设置拉取数据的最小字节数
//        调整一下代码 调整fetch.max.bytes大小，默认是50m。
//        调整max.poll.records大小，默认是500条
        //properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "50"); // 5MB
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        //    properties.setProperty("fetch.max.wait.ms", "500"); // 设置等待时间
        properties.setProperty("flink.streaming.kafka.poll-timeout", "100"); // 设置消费者的等待时间
        properties.setProperty("auto.commit.interval.ms", "10000"); // 设置自动提交offset的时间间隔
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(),properties);
    }

    public static FlinkKafkaProducer<String> sendKafkaDs(String topic, String groupID){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);




    }
}
