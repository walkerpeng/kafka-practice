package me.walker.producerdemo;

public class KafkaProducerDemo {
    public static final String TOPIC = "testTopic";

    public static void main(String[] args) {
        boolean isAsync = false;
        SampleProducer producerThread = new SampleProducer(TOPIC,isAsync);
        producerThread.start();
    }
}
