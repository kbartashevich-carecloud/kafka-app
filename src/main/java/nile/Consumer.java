package nile;

import java.util.*;

import org.apache.kafka.clients.consumer.*;

public class Consumer {

  private final KafkaConsumer<String, String> consumer;               1
  private final String topic;

  public Consumer(String servers, String groupId, String topic) {
    this.consumer = new KafkaConsumer<String, String>(
      createConfig(servers, groupId));
    this.topic = topic;
  }

  public void run(IProducer producer) {
    this.consumer.subscribe(Arrays.asList(this.topic));               2
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);   3
      for (ConsumerRecord<String, String> record : records) {
        producer.process(record.value());                             4
      }
    }
  }

  private static Properties createConfig(String servers, String groupId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", servers);
    props.put("group.id", groupId);                                   5
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer");    1
    props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer");    1
    return props;
  }
}