package cn.nwcdcloud.jingamz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
/*
public class HelloProducer {
    public static void main(String[] args) {
        long events = Long.parseLong(args[0]);
        Random rnd = new Random();

        Properties props = new Properties();

        props.put("bootstrap.servers", "52.80.237.218:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");

        ProducerConfig config = new ProducerConfig(props);

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(config);

    }
}
*/

public class HelloProducer {

    public static void main(String[] args) throws Exception{

        // Check arguments length value
        /*
        if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }
        */
        //Assign topicName to string variable
        String topicName = "jingamz1111";

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "ec2-52-80-215-42.cn-north-1.compute.amazonaws.com.cn:9092");
        props.put("batch.size", 16384);
/*
        //Set acknowledgements for producer requests.
        props.put("acks", "all");

                //If the request fails, the producer can automatically retry,
                props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
*/
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);
/*
        for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        bin/kafka-console-consumer.sh --bootstrap-server 52.80.237.218:9092 --topic test --from-beginning

        RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
        System.out.println("broker返回消息发送信息" + metadata);

 */
        //producer.send(new ProducerRecord<String, String>(topicName, "Hello Jingamz !!!"));
        ProducerRecord<String,String> record = new ProducerRecord<>(topicName,"JJJJJJIIIIIIIINNNNNNNNGGGGGGGGG1111");
        try {
             RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
             System.out.println("broker返回消息发送信息：" + metadata);
        } catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("Message sent successfully");
        producer.close();
    }
}