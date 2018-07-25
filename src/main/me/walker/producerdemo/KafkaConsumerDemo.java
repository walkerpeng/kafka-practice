package me.walker.producerdemo;


public class KafkaConsumerDemo {
    public static void main(String[] args) {
        SampleConsumer consumerThread = new SampleConsumer("testTopic");
        consumerThread.start();
    }
}
