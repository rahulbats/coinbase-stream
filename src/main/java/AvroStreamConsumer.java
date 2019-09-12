import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ksql.avro_schemas.KsqlDataSourceSchema;
import objects.CoinbaseSnapshot;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import serdes.RawDeserializer;
import serdes.RawSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class AvroStreamConsumer {
    public static void main(String args[]) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-snapshot");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://my-schema-registry:8081");

        StreamsBuilder builder = new StreamsBuilder();
        Serde<CoinbaseSnapshot> snapshotSerde = Serdes.serdeFrom(new RawSerializer(), new RawDeserializer());

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://localhost:8081");

        final Serde<KsqlDataSourceSchema> valueSpecificAvroSerde = new SpecificAvroSerde();
        valueSpecificAvroSerde.configure(serdeConfig, false);

        KStream<String, KsqlDataSourceSchema> snaps = builder.stream("SNAPSHOTS_AVRO", Consumed.with(Serdes.String(), valueSpecificAvroSerde));

        snaps.peek((key, value) ->  {
            System.out.println(value);
        });


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }
}
