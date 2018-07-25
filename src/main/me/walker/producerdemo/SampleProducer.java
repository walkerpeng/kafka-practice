package me.walker.producerdemo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SampleProducer extends Thread{

    private final KafkaProducer<Integer,String> producer;

    private final String topic;

    private final Boolean isAsync;

    private final String KAFKA_SERVER_URL = "192.168.83.116";
    private final int KAFKA_SERVER_PORT = 9092;
    private final String CLIENT_ID = "SampleProducer";


    public SampleProducer(String topic,Boolean isAsync){
        Properties properties = new Properties();
        properties.put("bootstrap.servers",KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id",CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) {
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else {
                try {
                    producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }

    class DemoCallBack implements Callback{

        private final long startTime;
        private final int key;
        private final String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }


        public void onCompletion(RecordMetadata metadata, Exception e) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                System.out.println("message(" + key + "," + message + ")sent to partition(" + metadata.partition() + ")," + "offset(" + metadata.offset() + ")in" + elapsedTime + " ms");
            } else {
                e.printStackTrace();
            }
        }
    }
}
