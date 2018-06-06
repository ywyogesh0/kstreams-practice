package com.ywyogesh0.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-1");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Long> kTable = builder.stream("wc-input-1", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(text -> text.toLowerCase())
                .flatMapValues(lowerCaseText -> Arrays.asList(lowerCaseText.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count(Materialized.as("Word_Count"));

        kTable.toStream().to("wc-output-1", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        for (StreamsMetadata metadata : streams.allMetadataForStore("Word_Count")) {
            System.out.println(metadata.toString());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
