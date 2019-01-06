package com.demo.swapnil;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.StreamsConfig.*;

public class SquareTheNumberApp {
  public static void main(String[] args) {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    final Properties props = new Properties();
    props.setProperty(APPLICATION_ID_CONFIG, "square-the-number-stream");
    props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final KStream<String, String> stream = streamsBuilder.stream("original-number");
    stream
        .map(((key, value) -> {
          try {
            int number = Integer.parseInt(value);
            return KeyValue.pair(key, number * number);
          } catch(NumberFormatException e) {
            return KeyValue.pair(key, 0);
          }
        }))
        .map((key, value) -> KeyValue.pair(key, Integer.toString(value)))
        .to("squared-number");

    final Topology topology = streamsBuilder.build();
    final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

    Runtime.getRuntime().addShutdownHook(new Thread("shutdown-thread") {
      @Override
      public void run() {
        super.run();
        kafkaStreams.close();
        countDownLatch.countDown();
      }
    });

    kafkaStreams.start();
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      System.exit(1);
    }
  }
}
