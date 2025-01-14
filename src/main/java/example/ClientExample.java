package example;

import java.lang.System;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.time.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

public class ClientExample {
  public static void main(String[] args) {
    try {
      String topic = "netflix_audience_behaviour_uk_movies";
      final Properties config = readConfig("client.properties");

      // produce(topic, config);
      consume(topic, config);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Properties readConfig(final String configFile) throws IOException {
    // reads the client configuration from client.properties
    // and returns it as a Properties object
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }

    final Properties config = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      config.load(inputStream);
    }

    return config;
  }

  public static void produce(String topic, Properties config) throws IOException {
    Schema schema = new Schema.Parser().parse(new File("src/main/avro/vodclickstream.avsc"));

    // sets the message serializers
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

    Producer<Integer, GenericRecord> producer = new KafkaProducer<>(config);

    try (BufferedReader reader = new BufferedReader(new FileReader("src/data/vodclickstream_uk_movies_03_sample.csv"))) {

      CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
        .setHeader("id","datetime","duration","title","genres","release_date","movie_id","user_id")
        .setQuoteMode(QuoteMode.MINIMAL)
        .setSkipHeaderRecord(true).get();
      
      Iterable<CSVRecord> records = csvFormat.parse(reader);
      
      for (CSVRecord record : records) {
        // Create AVRO record for each CSV row
        GenericRecord avroRecord = new GenericData.Record(schema);

        Integer id = Integer.parseInt(record.get("id"));

        avroRecord.put("id", id);
        avroRecord.put("datetime", record.get("datetime"));
        avroRecord.put("duration", Float.parseFloat(record.get("duration")));
        avroRecord.put("title", record.get("title"));
        avroRecord.put("genres", Arrays.asList(record.get("genres").split(",")));
        avroRecord.put("release_date", record.get("release_date"));
        avroRecord.put("movie_id", record.get("movie_id"));
        avroRecord.put("user_id", record.get("user_id"));

        // Send record to Kafka
        // producer.send(new ProducerRecord<>(topic, id, avroRecord));

        producer.send(new ProducerRecord<>(topic, id, avroRecord), (metadata, exception) -> {
          if (exception != null) {
              System.err.println("Error producing message: " + exception.getStackTrace());
              exception.printStackTrace();
          } else {
              System.out.println("Produced message to topic " + topic + ": " + avroRecord);
              System.out.println("Message sent to partition: " + metadata.partition() + ", offset: " + metadata.offset());
          }
        });

        producer.flush();
        // System.out.println("Produced message to topic " + topic + ": " + avroRecord);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    // closes the producer connection
    producer.close();
  }

  public static void consume (String topic, Properties config) {
    // sets the group ID, offset and message deserializers
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "java-group-1");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

    // creates a new consumer instance and subscribes to messages from the topic
    KafkaConsumer<Integer, GenericRecord> consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Arrays.asList(topic));

    while (true) {
      // polls the consumer for new messages and prints them
      ConsumerRecords<Integer, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<Integer, GenericRecord> record : records) {
        System.out.println(
          String.format(
            "Consumed message from topic %s: key = %s value = %s", topic, record.key(), record.value()
          )
        );
      }
    }
  }
}