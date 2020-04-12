package com.example.kafkastream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

@SpringBootApplication
public class KafkastreamApplication {

	Logger logger = LoggerFactory.getLogger(KafkastreamApplication.class);
	@Qualifier("kafkaStreamsConfiguration")
	@Autowired
	private KafkaStreamsConfiguration kafkaStreamsConfiguration;


	public static void main(String[] args) {
		SpringApplication.run(KafkastreamApplication.class, args);

	}

//	@Bean
//	public Consumer<KStream<Object, String>> process() {
//
//		return input ->
//				input.foreach((key, value) -> {
////					System.out.println("Key: " + key + " Value: " + value);
//					logger.info("\nconsumer >> "+"Key: " + key + " Value: " + value+"\n");
//				});
//	}

	@Bean
	public Function<KStream<String,String>, KStream<String,Long>> teststream() {

		return input -> input.peek((key,value) -> System.out.println("key="+key+",value="+value))
				.groupBy((key, word) -> word)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
				.toStream();
	}
	//				.flatMapValues(value ->
//						Arrays.asList(value.toLowerCase().split("\\W+")))
//				.map((key, value) -> new KeyValue<>(value, value))
//				.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
//				.windowedBy(TimeWindows.of(5000))
//				.count(Materialized.as("word-counts-state-store"))
//				.toStream()
//				.map((key, value) -> new KeyValue<>(key.key(),
//						new WordCount(key.key(), value, new Date(key.window().start(),
//								new Date(key.window().end())))));

}
