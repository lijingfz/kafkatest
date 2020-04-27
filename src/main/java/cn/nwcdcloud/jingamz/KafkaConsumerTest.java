package cn.nwcdcloud.jingamz;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerTest {
    public static final String brokerList = "b-3.jingamz-0416.m7xnks.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092,b-4.jingamz-0416.m7xnks.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092,b-2.jingamz-0416.m7xnks.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092";
    public static final String topic = "jingamz111";
    public static final String groupID = "group.jingamz";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {

        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupID);
        props.put("client.id", "consumer.client.id.demo");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        return props;
    }

    public static void main(String[] args) {
        final int minBatchSize = 10;
        List<ConsumerRecord> buffer = new ArrayList<>();
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        //TopicPartition tp = new TopicPartition(topic,0);
        //consumer.assign(Arrays.asList(tp));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Topic =" + record.topic() + ",partition:" + record.partition() + ",offset:" + record.offset());
                    System.out.println("Key = " + record.key() + ", value = " + record.value());
                    //consumer.commitSync();
                    buffer.add(record);
                    if (buffer.size()>=minBatchSize){
                        consumer.commitSync();
                        System.out.println("Commit per 10 recordes !!");
                        buffer.clear();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Occur exception !" + e);
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }
}