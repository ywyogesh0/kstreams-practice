package com.ywyogesh0.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class FColorApp {

    private enum fColors {
        RED, BLUE, GREEN;

        public static boolean isFavColor(String color) {
            for (fColors key : fColors.values()) {
                if (key.name().toLowerCase().equals(color))
                    return true;
            }

            return false;
        }
    }

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fc-5");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.<String, String>stream("fc-i-5")
                .filter((nullKey, value) -> value.contains(",") && value.split(",").length == 2)

                .map((nullKey, value) ->
                        new KeyValue<>(value.split(",")[0].toLowerCase(), value.split(",")[1].toLowerCase()))

                .filter((userId, color) -> fColors.isFavColor(color))
                .to("fc-t-5");

        streamsBuilder.<String, String>table("fc-t-5")
                .groupBy((userId, color) -> new KeyValue<>(color, color))

                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("Count")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))

                .toStream()
                .to("fc-o-5", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
